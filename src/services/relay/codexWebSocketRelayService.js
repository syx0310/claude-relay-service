/**
 * Codex WebSocket Relay Service
 *
 * WebSocket 代理服务：接受客户端 (Codex CLI) 的 WebSocket 连接，
 * 认证后建立到上游 Codex API 的 WebSocket 连接，双向转发消息。
 *
 * 关键特性：
 * - 懒连接：首条客户端消息时才建立上游 WS（需要 model 字段选账户）
 * - 429/401 账户切换：关闭上游 WS，保持客户端 WS，下次消息重选账户
 * - Usage 追踪：解析 response.completed 事件记录 token 使用量
 * - store 字段：openai 账户强制 false，openai-responses 透传
 */

const WebSocket = require('ws')
const crypto = require('crypto')
const logger = require('../../utils/logger')
const config = require('../../../config/config')
const ProxyHelper = require('../../utils/proxyHelper')
const apiKeyService = require('../apiKeyService')
const unifiedOpenAIScheduler = require('../scheduler/unifiedOpenAIScheduler')
const openaiAccountService = require('../account/openaiAccountService')
const openaiResponsesAccountService = require('../account/openaiResponsesAccountService')
const { adaptCodexRequestBody } = require('../../utils/codexRequestAdapter')

const upstreamErrorHelper = require('../../utils/upstreamErrorHelper')

// Codex CLI User-Agent 检测正则
const CODEX_CLI_UA_PATTERN = /^(codex_vscode|codex_cli_rs|codex_exec)\/[\d.]+/i

// 超时常量（与 Codex 客户端 provider.rs stream_idle_timeout_ms=300000 对齐）
const IDLE_TIMEOUT_MS = parseInt(process.env.CODEX_WS_IDLE_TIMEOUT_MS) || 300000 // 5 min
const MAX_LIFETIME_MS = parseInt(process.env.CODEX_WS_MAX_LIFETIME_MS) || 3600000 // 60 min
const UPSTREAM_CONNECT_TIMEOUT_MS = 30000 // 30s

class CodexWebSocketRelayService {
  constructor() {
    this.wss = null
    this.connections = new Map()
  }

  initialize() {
    this.wss = new WebSocket.Server({ noServer: true, perMessageDeflate: true })
    logger.info('🔌 CodexWebSocketRelayService initialized')
  }

  /**
   * HTTP upgrade 入口，从 app.js server.on('upgrade') 调用
   */
  async handleUpgrade(req, socket, head) {
    try {
      // 提取 API key（复用 auth.js extractApiKey 的逻辑模式）
      const apiKey = this._extractApiKey(req)

      if (!apiKey || apiKey.length < 10) {
        this._rejectUpgrade(socket, 401, 'Missing or invalid API key')
        return
      }

      const validation = await apiKeyService.validateApiKey(apiKey)
      if (!validation.valid) {
        this._rejectUpgrade(socket, 401, validation.error || 'Invalid API key')
        return
      }

      if (!apiKeyService.hasPermission(validation.keyData?.permissions, 'openai')) {
        this._rejectUpgrade(socket, 403, 'No OpenAI permission for this API key')
        return
      }

      // 完成 WebSocket 握手
      this.wss.handleUpgrade(req, socket, head, (clientWs) => {
        this._onClientConnected(clientWs, req, validation.keyData)
      })
    } catch (error) {
      logger.error('❌ [CodexWS] Upgrade error:', error.message)
      this._rejectUpgrade(socket, 500, 'Internal server error')
    }
  }

  /**
   * 从请求头提取 API key（支持多种格式）
   */
  _extractApiKey(req) {
    const candidates = [
      req.headers['x-api-key'],
      req.headers['authorization'],
      req.headers['api-key']
    ]

    for (const candidate of candidates) {
      if (typeof candidate !== 'string' || !candidate.trim()) {
        continue
      }
      let trimmed = candidate.trim()
      if (/^Bearer\s+/i.test(trimmed)) {
        trimmed = trimmed.replace(/^Bearer\s+/i, '').trim()
      }
      if (trimmed) {
        return trimmed
      }
    }

    // 查询参数 fallback
    const url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`)
    return url.searchParams.get('key') || ''
  }

  /**
   * 拒绝 WebSocket upgrade，写原始 HTTP 响应
   */
  _rejectUpgrade(socket, statusCode, message) {
    if (socket.destroyed) {
      return
    }
    const body = JSON.stringify({
      error: { message, type: 'authentication_error', code: statusCode }
    })
    try {
      socket.write(
        `HTTP/1.1 ${statusCode} ${message}\r\n` +
          `Content-Type: application/json\r\n` +
          `Content-Length: ${Buffer.byteLength(body)}\r\n` +
          `Connection: close\r\n` +
          `\r\n${body}`
      )
    } catch {
      // socket may already be closed
    }
    socket.destroy()
  }

  /**
   * WebSocket 握手成功后的连接初始化
   */
  _onClientConnected(clientWs, req, apiKeyData) {
    const connState = {
      apiKeyData,
      headers: req.headers,
      userAgent: req.headers['user-agent'] || '',
      isCodexCLI: CODEX_CLI_UA_PATTERN.test(req.headers['user-agent'] || ''),
      // 上游连接状态（429/401 后重置为 null）
      upstreamWs: null,
      accountId: null,
      accountType: null,
      account: null,
      sessionHash: null,
      proxy: null,
      // Turn 跟踪
      currentTurnHasOutput: false,
      // 429 静默切号
      lastClientMessage: null,
      retryCount: 0,
      // 计时器
      idleTimer: null,
      lifetimeTimer: null,
      isClosing: false
    }

    this.connections.set(clientWs, connState)

    // 最大连接时长
    connState.lifetimeTimer = setTimeout(() => {
      logger.info(`⏰ [CodexWS] Connection exceeded max lifetime (${MAX_LIFETIME_MS}ms)`)
      this._closeConnection(clientWs, 1000, 'Connection lifetime exceeded')
    }, MAX_LIFETIME_MS)

    this._resetIdleTimer(clientWs, connState)

    clientWs.on('message', (data) => this._handleClientMessage(clientWs, data))
    clientWs.on('close', () => this._cleanup(clientWs))
    clientWs.on('error', (err) => {
      logger.error(`❌ [CodexWS] Client WS error: ${err.message}`)
      this._cleanup(clientWs)
    })

    logger.info(
      `🔌 [CodexWS] Client connected, key=${apiKeyData.id} (${apiKeyData.name}), ` +
        `codexCLI=${connState.isCodexCLI}`
    )
  }

  /**
   * 处理客户端发送的消息
   */
  async _handleClientMessage(clientWs, data) {
    const connState = this.connections.get(clientWs)
    if (!connState || connState.isClosing) {
      return
    }

    this._resetIdleTimer(clientWs, connState)

    const messageStr = data.toString()
    let parsed
    try {
      parsed = JSON.parse(messageStr)
    } catch {
      // 非 JSON，如果上游存在则原样转发
      if (connState.upstreamWs?.readyState === WebSocket.OPEN) {
        connState.upstreamWs.send(messageStr)
      }
      return
    }

    // 新 turn 开始（每条消息都是独立的 Responses API 请求）
    connState.currentTurnHasOutput = false

    // 如果没有上游连接，建立连接（懒连接 / 429后重建）
    if (!connState.upstreamWs || connState.upstreamWs.readyState !== WebSocket.OPEN) {
      connState.upstreamWs = null
      try {
        await this._establishUpstream(clientWs, connState, parsed)
      } catch (error) {
        logger.error(`❌ [CodexWS] Failed to establish upstream: ${error.message}`)
        const errorEvent = {
          type: 'error',
          status: error.statusCode || 502,
          error: {
            type: 'relay_error',
            message: error.message || 'Failed to connect to upstream',
            code: 'upstream_connection_failed'
          }
        }
        this._sendToClient(clientWs, JSON.stringify(errorEvent))

        // 如果是无可用账户，关闭客户端连接
        if (error.statusCode === 402 || error.statusCode === 403) {
          this._closeConnection(clientWs, 1008, error.message)
        }
        return
      }
    }

    // 应用请求适配
    let messageToSend = messageStr
    if (!connState.isCodexCLI) {
      const adapterResult = adaptCodexRequestBody(parsed, {
        isCodexCLI: false,
        adapterConfig: config?.openai?.codexAdapter,
        defaultInstructionsText: undefined // 让 adapter 使用配置的默认值
      })
      if (adapterResult.applied) {
        parsed = adapterResult.body
      }
    }

    // store 字段处理
    if (connState.accountType !== 'openai-responses') {
      parsed.store = false
    }

    messageToSend = JSON.stringify(parsed)

    // 保存处理后的消息（用于 429 静默切号重发）
    connState.lastClientMessage = messageToSend
    connState.retryCount = 0

    // 转发给上游
    if (connState.upstreamWs?.readyState === WebSocket.OPEN) {
      connState.upstreamWs.send(messageToSend)
    }
  }

  /**
   * 建立上游 WebSocket 连接
   */
  async _establishUpstream(clientWs, connState, firstMessage) {
    const requestedModel = firstMessage.model || null
    const sessionId = firstMessage.session_id || firstMessage.prompt_cache_key || null
    const sessionHash = sessionId
      ? crypto.createHash('sha256').update(sessionId).digest('hex')
      : null

    connState.sessionHash = sessionHash

    // 选择账户
    let result
    try {
      result = await unifiedOpenAIScheduler.selectAccountForApiKey(
        connState.apiKeyData,
        sessionHash,
        requestedModel
      )
    } catch (error) {
      const err = new Error(error.message || 'No available account')
      err.statusCode = 402
      throw err
    }

    if (!result?.accountId) {
      const err = new Error('No available OpenAI account found')
      err.statusCode = 402
      throw err
    }

    connState.accountId = result.accountId
    connState.accountType = result.accountType

    let upstreamUrl
    const upstreamHeaders = {}
    let proxy = null

    if (result.accountType === 'openai-responses') {
      const account = await openaiResponsesAccountService.getAccount(result.accountId)
      if (!account?.apiKey) {
        const err = new Error('Account has no valid apiKey')
        err.statusCode = 403
        throw err
      }
      connState.account = account

      // 构建上游 WS URL
      const baseApi = (account.baseApi || '').replace(/\/+$/, '')
      let httpUrl = `${baseApi}/v1/responses`
      // 防止 /v1/v1 重复
      if (baseApi.endsWith('/v1')) {
        httpUrl = `${baseApi}/responses`
      }
      upstreamUrl = httpUrl.replace(/^https:/, 'wss:').replace(/^http:/, 'ws:')

      upstreamHeaders['Authorization'] = `Bearer ${account.apiKey}`

      if (account.proxy) {
        try {
          proxy = typeof account.proxy === 'string' ? JSON.parse(account.proxy) : account.proxy
        } catch {
          /* ignore */
        }
      }
    } else {
      // openai (ChatGPT OAuth) 账户
      const account = await openaiAccountService.getAccount(result.accountId)
      if (!account?.accessToken) {
        const err = new Error('Account has no valid accessToken')
        err.statusCode = 403
        throw err
      }

      // Token 过期自动刷新
      if (openaiAccountService.isTokenExpired(account)) {
        if (account.refreshToken) {
          await openaiAccountService.refreshAccountToken(result.accountId)
          const refreshed = await openaiAccountService.getAccount(result.accountId)
          if (!refreshed?.accessToken) {
            const err = new Error('Token refresh failed')
            err.statusCode = 403
            throw err
          }
          connState.account = refreshed
        } else {
          const err = new Error('Token expired and no refresh token')
          err.statusCode = 403
          throw err
        }
      } else {
        connState.account = account
      }

      const accessToken = openaiAccountService.decrypt(connState.account.accessToken)
      upstreamUrl = 'wss://chatgpt.com/backend-api/codex/responses'

      upstreamHeaders['Authorization'] = `Bearer ${accessToken}`
      upstreamHeaders['chatgpt-account-id'] =
        connState.account.accountId || connState.account.chatgptUserId || result.accountId
      upstreamHeaders['host'] = 'chatgpt.com'

      if (connState.account.proxy) {
        try {
          proxy =
            typeof connState.account.proxy === 'string'
              ? JSON.parse(connState.account.proxy)
              : connState.account.proxy
        } catch {
          /* ignore */
        }
      }
    }

    connState.proxy = proxy

    // 透传客户端头
    const clientHeaders = connState.headers || {}
    const passthroughKeys = [
      'openai-beta',
      'originator',
      'session_id',
      'user-agent',
      'version',
      'x-codex-turn-state',
      'x-codex-beta-features',
      'x-codex-turn-metadata'
    ]
    for (const key of passthroughKeys) {
      if (clientHeaders[key]) {
        upstreamHeaders[key] = clientHeaders[key]
      }
    }

    // 构建 WS 连接选项
    const wsOptions = {
      headers: upstreamHeaders,
      perMessageDeflate: true,
      handshakeTimeout: UPSTREAM_CONNECT_TIMEOUT_MS
    }

    const proxyAgent = ProxyHelper.createProxyAgent(proxy)
    if (proxyAgent) {
      wsOptions.agent = proxyAgent
      logger.info(`🌐 [CodexWS] Using proxy: ${ProxyHelper.getProxyDescription(proxy)}`)
    }

    logger.info(
      `🔗 [CodexWS] Connecting upstream: ${upstreamUrl}, ` +
        `account=${connState.account?.name || connState.accountId}, ` +
        `type=${connState.accountType}`
    )

    // 建立连接（Promise 封装）
    return new Promise((resolve, reject) => {
      const upstreamWs = new WebSocket(upstreamUrl, wsOptions)

      const connectTimeout = setTimeout(() => {
        upstreamWs.terminate()
        const err = new Error('Upstream WebSocket connection timeout')
        err.statusCode = 502
        reject(err)
      }, UPSTREAM_CONNECT_TIMEOUT_MS)

      upstreamWs.on('open', () => {
        clearTimeout(connectTimeout)
        connState.upstreamWs = upstreamWs
        logger.info(`✅ [CodexWS] Upstream connected: ${upstreamUrl}`)

        // 设置上游消息处理
        upstreamWs.on('message', (msg) => {
          this._handleUpstreamMessage(clientWs, connState, msg)
        })

        upstreamWs.on('close', (code, reason) => {
          const reasonStr = reason?.toString() || ''
          logger.info(`🔌 [CodexWS] Upstream closed: code=${code} reason=${reasonStr}`)
          this._handleUpstreamClose(clientWs, connState, code, reasonStr)
        })

        upstreamWs.on('error', (err) => {
          logger.error(`❌ [CodexWS] Upstream error: ${err.message}`)
          this._handleUpstreamError(clientWs, connState, err)
        })

        resolve()
      })

      upstreamWs.on('error', (err) => {
        clearTimeout(connectTimeout)
        logger.error(`❌ [CodexWS] Upstream connect failed: ${err.message}`)
        const error = new Error(`Upstream connection failed: ${err.message}`)
        error.statusCode = 502
        reject(error)
      })
    })
  }

  /**
   * 处理上游 WS 消息
   */
  _handleUpstreamMessage(clientWs, connState, data) {
    if (clientWs.readyState !== WebSocket.OPEN) {
      return
    }

    const messageStr = data.toString()

    // 检查是否为 429 error event — 静默切号，不转发给客户端
    try {
      const parsed = JSON.parse(messageStr)
      if (parsed.type === 'error' && (parsed.status === 429 || parsed.status_code === 429)) {
        this._handleSilentAccountSwitch(clientWs, connState, parsed, messageStr).catch((err) => {
          logger.error(`❌ [CodexWS] Silent account switch failed: ${err.message}`)
        })
        return // 不转发 429 给客户端
      }
    } catch {
      // 非 JSON，继续正常转发
    }

    // 正常转发给客户端
    this._sendToClient(clientWs, messageStr)

    // 重置空闲计时器
    this._resetIdleTimer(clientWs, connState)

    // 异步解析事件（best-effort）
    this._processUpstreamEvent(clientWs, connState, messageStr).catch((err) => {
      logger.error(`❌ [CodexWS] Error processing upstream event: ${err.message}`)
    })
  }

  /**
   * 解析上游事件，处理 usage/error/rate_limits
   */
  async _processUpstreamEvent(clientWs, connState, messageStr) {
    let event
    try {
      event = JSON.parse(messageStr)
    } catch {
      return
    }

    const eventType = event.type || event.kind || ''

    // response.completed — 提取 usage
    if (eventType === 'response.completed' && event.response) {
      connState.currentTurnHasOutput = true
      const { usage } = event.response
      const actualModel = event.response.model

      if (usage) {
        try {
          const totalInputTokens = usage.input_tokens || 0
          const outputTokens = usage.output_tokens || 0
          const cacheReadTokens = usage.input_tokens_details?.cached_tokens || 0
          const cacheCreateTokens = usage.input_tokens_details?.cache_creation_input_tokens || 0
          const actualInputTokens = Math.max(0, totalInputTokens - cacheReadTokens)
          const modelToRecord = actualModel || 'gpt-4'

          await apiKeyService.recordUsage(
            connState.apiKeyData.id,
            actualInputTokens,
            outputTokens,
            cacheCreateTokens,
            cacheReadTokens,
            modelToRecord,
            connState.accountId,
            connState.accountType
          )

          logger.info(
            `📊 [CodexWS] Usage: input=${totalInputTokens} (actual=${actualInputTokens}+cache=${cacheReadTokens}), ` +
              `output=${outputTokens}, model=${modelToRecord}, account=${connState.accountId}`
          )

          // 更新账户 usage 统计
          if (connState.accountType === 'openai-responses') {
            await openaiResponsesAccountService
              .updateAccountUsage(connState.accountId, totalInputTokens + outputTokens)
              .catch((err) => logger.error('Account usage update failed:', err.message))
          }
        } catch (err) {
          logger.error(`❌ [CodexWS] Failed to record usage: ${err.message}`)
        }
      }

      // 成功后清除限流状态
      try {
        const isRateLimited = await unifiedOpenAIScheduler.isAccountRateLimited(connState.accountId)
        if (isRateLimited) {
          await unifiedOpenAIScheduler.removeAccountRateLimit(
            connState.accountId,
            connState.accountType
          )
          logger.info(
            `✅ [CodexWS] Cleared rate limit for account ${connState.accountId} after success`
          )
        }
      } catch {
        // ignore
      }
    }

    // response.created / delta 事件 — 标记 turn 有输出
    if (
      eventType === 'response.created' ||
      eventType.includes('.delta') ||
      eventType === 'response.output_item.added'
    ) {
      connState.currentTurnHasOutput = true
    }

    // error 事件 — 429/401 处理
    if (eventType === 'error') {
      const status = event.status || event.status_code
      await this._handleUpstreamErrorEvent(clientWs, connState, status, event)
    }

    // codex.rate_limits 事件
    if (eventType === 'codex.rate_limits' && connState.accountType === 'openai') {
      try {
        const snapshot = this._extractRateLimitSnapshot(event)
        if (snapshot) {
          await openaiAccountService.updateCodexUsageSnapshot(connState.accountId, snapshot)
        }
      } catch (err) {
        logger.error(`❌ [CodexWS] Failed to update codex usage snapshot: ${err.message}`)
      }
    }
  }

  /**
   * 处理上游 error 事件（429/401 等）
   * 注意：error 事件已在 _handleUpstreamMessage 中转发给客户端
   */
  async _handleUpstreamErrorEvent(clientWs, connState, status, event) {
    if (status === 429) {
      const resetsInSeconds = event.error?.resets_in_seconds || null
      logger.warn(
        `🚫 [CodexWS] Rate limit (429) for account ${connState.accountId}${
          resetsInSeconds ? `, resets in ${resetsInSeconds}s` : ''
        }`
      )

      try {
        await unifiedOpenAIScheduler.markAccountRateLimited(
          connState.accountId,
          connState.accountType,
          connState.sessionHash,
          resetsInSeconds
        )
      } catch {
        // ignore
      }

      // 关闭上游、清空状态、保持客户端 WS
      this._detachUpstream(connState)
    } else if (status === 401) {
      logger.warn(`🚫 [CodexWS] Auth error (401) for account ${connState.accountId}`)

      const oaiAutoProtectionDisabled =
        connState.account?.disableAutoProtection === true ||
        connState.account?.disableAutoProtection === 'true'
      if (!oaiAutoProtectionDisabled) {
        try {
          await upstreamErrorHelper.markTempUnavailable(
            connState.accountId,
            connState.accountType,
            401
          )
        } catch {
          // ignore
        }
      }

      // 删除 session 映射，确保下次消息重选账户
      if (connState.sessionHash) {
        await unifiedOpenAIScheduler._deleteSessionMapping(connState.sessionHash).catch(() => {})
      }
      this._detachUpstream(connState)
    }
  }

  /**
   * 429 静默切号：拦截 429 error event，自动切换账户重发请求，客户端无感
   */
  async _handleSilentAccountSwitch(clientWs, connState, errorEvent, rawMessage) {
    const MAX_SWITCH_RETRIES = 3
    connState.retryCount = (connState.retryCount || 0) + 1

    if (connState.retryCount > MAX_SWITCH_RETRIES || !connState.lastClientMessage) {
      // 超过重试次数或无消息可重发 → 转发 429 给客户端
      logger.warn(
        `🚫 [CodexWS] 429 exhausted ${connState.retryCount} retries, forwarding to client`
      )
      this._sendToClient(clientWs, rawMessage)
      this._processUpstreamEvent(clientWs, connState, rawMessage).catch(() => {})
      return
    }

    const resetsInSeconds = errorEvent.error?.resets_in_seconds || null
    logger.info(
      `🔄 [CodexWS] 429 on account ${connState.accountId}, silent switch attempt ${connState.retryCount}/${MAX_SWITCH_RETRIES}${resetsInSeconds ? ` (resets in ${resetsInSeconds}s)` : ''}`
    )

    // 1. 标记账户限流（内部会删 session 映射）
    try {
      await unifiedOpenAIScheduler.markAccountRateLimited(
        connState.accountId,
        connState.accountType,
        connState.sessionHash,
        resetsInSeconds
      )
    } catch {
      // ignore
    }

    // 2. 断开当前上游
    this._detachUpstream(connState)

    // 3. 用 lastClientMessage 重建上游连接并重发
    try {
      const parsed = JSON.parse(connState.lastClientMessage)
      await this._establishUpstream(clientWs, connState, parsed)

      if (connState.upstreamWs?.readyState === WebSocket.OPEN) {
        connState.upstreamWs.send(connState.lastClientMessage)
        logger.info(`✅ [CodexWS] Silent switch to account ${connState.accountId}, message resent`)
      }
    } catch (error) {
      // 无可用账户 → 转发 429 给客户端
      logger.warn(`🚫 [CodexWS] No account available for silent switch: ${error.message}`)
      this._sendToClient(clientWs, rawMessage)
    }
  }

  /**
   * 上游 WS 关闭事件处理
   */
  _handleUpstreamClose(clientWs, connState, code, reason) {
    // 关闭上游、保持客户端 WS（下次消息时重建）
    this._detachUpstream(connState)

    // 如果是 connection_limit_reached，客户端已收到 error 事件
    // 非正常关闭时通知客户端
    if (code !== 1000 && code !== 1001 && clientWs.readyState === WebSocket.OPEN) {
      const errorEvent = {
        type: 'error',
        status: 502,
        error: {
          type: 'upstream_closed',
          message: `Upstream closed: code=${code} reason=${reason}`,
          code: 'upstream_connection_closed'
        }
      }
      this._sendToClient(clientWs, JSON.stringify(errorEvent))
    }
  }

  /**
   * 上游 WS error 事件处理（网络错误等）
   */
  _handleUpstreamError(clientWs, connState, err) {
    this._detachUpstream(connState)

    if (clientWs.readyState === WebSocket.OPEN) {
      const errorEvent = {
        type: 'error',
        status: 502,
        error: {
          type: 'upstream_error',
          message: `Upstream connection error: ${err.message}`,
          code: 'upstream_connection_error'
        }
      }
      this._sendToClient(clientWs, JSON.stringify(errorEvent))
    }
  }

  /**
   * 断开上游连接、清空账户状态（保持客户端 WS）
   */
  _detachUpstream(connState) {
    if (connState.upstreamWs) {
      try {
        if (connState.upstreamWs.readyState === WebSocket.OPEN) {
          connState.upstreamWs.close(1000, 'Detaching')
        } else {
          connState.upstreamWs.terminate()
        }
      } catch {
        // ignore
      }
    }
    connState.upstreamWs = null
    connState.accountId = null
    connState.accountType = null
    connState.account = null
    connState.sessionHash = null
    connState.proxy = null
    connState.currentTurnHasOutput = false
  }

  /**
   * 从 codex.rate_limits 事件提取限流快照
   */
  _extractRateLimitSnapshot(event) {
    const headers = event.headers || event.data || {}
    const snapshot = {}
    const fields = [
      'x-codex-primary-used-percent',
      'x-codex-primary-reset-after-seconds',
      'x-codex-primary-window-minutes',
      'x-codex-secondary-used-percent',
      'x-codex-secondary-reset-after-seconds',
      'x-codex-secondary-window-minutes'
    ]
    let hasData = false
    for (const field of fields) {
      if (headers[field] !== undefined) {
        const camelCase = field
          .replace('x-codex-', '')
          .replace(/-([a-z])/g, (_, c) => c.toUpperCase())
        snapshot[camelCase] = headers[field]
        hasData = true
      }
    }
    return hasData ? snapshot : null
  }

  /**
   * 安全发送消息到客户端
   */
  _sendToClient(clientWs, message) {
    try {
      if (clientWs.readyState === WebSocket.OPEN) {
        clientWs.send(message)
      }
    } catch {
      // ignore
    }
  }

  /**
   * 重置空闲计时器
   */
  _resetIdleTimer(clientWs, connState) {
    if (connState.idleTimer) {
      clearTimeout(connState.idleTimer)
    }
    connState.idleTimer = setTimeout(() => {
      logger.info(`⏰ [CodexWS] Idle timeout (${IDLE_TIMEOUT_MS}ms)`)
      this._closeConnection(clientWs, 1000, 'Idle timeout')
    }, IDLE_TIMEOUT_MS)
  }

  /**
   * 关闭客户端和上游连接
   */
  _closeConnection(clientWs, code, reason) {
    const connState = this.connections.get(clientWs)
    if (!connState || connState.isClosing) {
      return
    }
    connState.isClosing = true

    // 关闭上游
    if (connState.upstreamWs) {
      try {
        if (connState.upstreamWs.readyState === WebSocket.OPEN) {
          connState.upstreamWs.close(code, reason)
        } else {
          connState.upstreamWs.terminate()
        }
      } catch {
        // ignore
      }
    }

    // 关闭客户端
    try {
      if (clientWs.readyState === WebSocket.OPEN) {
        clientWs.close(code, reason)
      } else if (clientWs.readyState !== WebSocket.CLOSED) {
        clientWs.terminate()
      }
    } catch {
      // ignore
    }

    this._cleanup(clientWs)
  }

  /**
   * 清理连接资源
   */
  _cleanup(clientWs) {
    const connState = this.connections.get(clientWs)
    if (!connState) {
      return
    }

    if (connState.idleTimer) {
      clearTimeout(connState.idleTimer)
    }
    if (connState.lifetimeTimer) {
      clearTimeout(connState.lifetimeTimer)
    }

    if (connState.upstreamWs) {
      try {
        connState.upstreamWs.terminate()
      } catch {
        // ignore
      }
    }

    this.connections.delete(clientWs)
    logger.info(
      `🧹 [CodexWS] Connection cleaned up, key=${connState.apiKeyData?.id}, ` +
        `active=${this.connections.size}`
    )
  }

  /**
   * 获取活跃连接数
   */
  getActiveConnectionCount() {
    return this.connections.size
  }

  /**
   * 优雅关闭所有连接
   */
  shutdown() {
    logger.info(`🛑 [CodexWS] Shutting down ${this.connections.size} connections`)
    for (const [clientWs] of this.connections) {
      this._closeConnection(clientWs, 1001, 'Server shutting down')
    }
    if (this.wss) {
      this.wss.close()
    }
  }
}

module.exports = new CodexWebSocketRelayService()
