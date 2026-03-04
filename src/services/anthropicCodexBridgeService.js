/**
 * ============================================================================
 * Anthropic → Codex (OpenAI Responses API) 桥接服务
 * ============================================================================
 *
 * 将 Anthropic Messages API 格式请求转换为 OpenAI Codex Responses API 格式，
 * 并将 Codex SSE 响应转换回 Anthropic SSE 格式返回给客户端（如 Claude Code）。
 *
 * 使用方式：客户端在 model 字段使用 "codex,<model>" vendor 前缀触发。
 *
 * 复用已有基础设施：
 * - unifiedOpenAIScheduler: 账户选择
 * - openaiAccountService / openaiResponsesAccountService: 账户管理
 * - ProxyHelper: 代理
 * - adaptCodexRequestBody: Codex 指令适配
 * - IncrementalSSEParser: SSE 解析
 * - apiKeyService: 使用量记录
 */

const crypto = require('crypto')
const axios = require('axios')
const logger = require('../utils/logger')
const config = require('../../config/config')
const ProxyHelper = require('../utils/proxyHelper')
const { IncrementalSSEParser } = require('../utils/sseParser')
const { adaptCodexRequestBody } = require('../utils/codexRequestAdapter')
const sessionHelper = require('../utils/sessionHelper')
const unifiedOpenAIScheduler = require('./scheduler/unifiedOpenAIScheduler')
const openaiAccountService = require('./account/openaiAccountService')
const openaiResponsesAccountService = require('./account/openaiResponsesAccountService')
const apiKeyService = require('./apiKeyService')
const { updateRateLimitCounters } = require('../utils/rateLimitHelper')
const { getSafeMessage } = require('../utils/errorSanitizer')

// Claude Code 通过 model name 查内部注册表来确定 context_window_size。
// 返回一个已知的 Claude 模型名，使其正确识别 200k 上下文窗口。
const CODEX_RESPONSE_MODEL_ALIAS = 'claude-sonnet-4-20250514'

// ============================================================================
// 辅助函数
// ============================================================================

/**
 * 写入 Anthropic SSE 事件
 */
function writeAnthropicSseEvent(res, event, data) {
  res.write(`event: ${event}\n`)
  res.write(`data: ${JSON.stringify(data)}\n\n`)
}

/**
 * 生成 Codex 风格的 tool call ID (call_xxxx)
 */
function generateCodexCallId() {
  return `call_${crypto.randomBytes(12).toString('hex')}`
}

/**
 * 归一化响应头为小写键
 */
function normalizeHeaders(headers = {}) {
  if (!headers || typeof headers !== 'object') {
    return {}
  }
  const normalized = {}
  for (const [key, value] of Object.entries(headers)) {
    if (!key) {
      continue
    }
    normalized[key.toLowerCase()] = Array.isArray(value) ? value[0] : value
  }
  return normalized
}

function toNumberSafe(value) {
  if (value === undefined || value === null || value === '') {
    return null
  }
  const num = Number(value)
  return Number.isFinite(num) ? num : null
}

/**
 * 提取 Codex usage 响应头
 */
function extractCodexUsageHeaders(headers) {
  const normalized = normalizeHeaders(headers)
  if (!normalized || Object.keys(normalized).length === 0) {
    return null
  }

  const snapshot = {
    primaryUsedPercent: toNumberSafe(normalized['x-codex-primary-used-percent']),
    primaryResetAfterSeconds: toNumberSafe(normalized['x-codex-primary-reset-after-seconds']),
    primaryWindowMinutes: toNumberSafe(normalized['x-codex-primary-window-minutes']),
    secondaryUsedPercent: toNumberSafe(normalized['x-codex-secondary-used-percent']),
    secondaryResetAfterSeconds: toNumberSafe(normalized['x-codex-secondary-reset-after-seconds']),
    secondaryWindowMinutes: toNumberSafe(normalized['x-codex-secondary-window-minutes']),
    primaryOverSecondaryPercent: toNumberSafe(
      normalized['x-codex-primary-over-secondary-limit-percent']
    )
  }

  const hasData = Object.values(snapshot).some((value) => value !== null)
  return hasData ? snapshot : null
}

// ============================================================================
// 请求转换: Anthropic → Codex
// ============================================================================

/**
 * 从 Anthropic system 数组中提取 instructions 文本
 * 过滤掉 billing header 和 system-reminder 元素
 */
function extractInstructionsFromSystem(system) {
  if (!system) {
    return ''
  }

  if (typeof system === 'string') {
    if (system.trim().startsWith('x-anthropic-billing-header')) {
      return ''
    }
    return system
  }

  if (Array.isArray(system)) {
    const parts = []
    for (const item of system) {
      if (!item || item.type !== 'text' || typeof item.text !== 'string') {
        continue
      }
      const trimmed = item.text.trim()
      // 过滤 billing header
      if (trimmed.startsWith('x-anthropic-billing-header')) {
        continue
      }
      // 过滤 system-reminder
      if (trimmed.startsWith('<system-reminder>')) {
        continue
      }
      parts.push(item.text)
    }
    return parts.join('\n\n')
  }

  return ''
}

/**
 * 转换 Anthropic messages → Codex input 数组
 * @returns {{ input: Array, toolIdMap: Map<string,string> }}
 */
function convertMessagesToCodexInput(messages) {
  const input = []
  // Claude toolu_xxx → Codex call_xxx 的映射
  const toolIdMap = new Map()

  for (const msg of messages) {
    if (msg.role === 'user') {
      const contentBlocks = normalizeContent(msg.content)
      for (const block of contentBlocks) {
        if (block.type === 'text') {
          input.push({ role: 'user', content: block.text })
        } else if (block.type === 'tool_result') {
          // tool_result → function_call_output
          const callId = toolIdMap.get(block.tool_use_id) || block.tool_use_id
          let outputText = ''
          if (typeof block.content === 'string') {
            outputText = block.content
          } else if (Array.isArray(block.content)) {
            outputText = block.content
              .filter((c) => c.type === 'text')
              .map((c) => c.text || '')
              .join('\n')
          }
          input.push({
            type: 'function_call_output',
            call_id: callId,
            output: outputText
          })
        }
      }
    } else if (msg.role === 'assistant') {
      const contentBlocks = normalizeContent(msg.content)
      for (const block of contentBlocks) {
        if (block.type === 'thinking') {
          // Skip thinking blocks — Codex has its own reasoning
          continue
        } else if (block.type === 'text') {
          input.push({
            type: 'message',
            role: 'assistant',
            content: [{ type: 'output_text', text: block.text }]
          })
        } else if (block.type === 'tool_use') {
          const callId = generateCodexCallId()
          toolIdMap.set(block.id, callId)
          input.push({
            type: 'function_call',
            call_id: callId,
            name: block.name,
            arguments: typeof block.input === 'string' ? block.input : JSON.stringify(block.input)
          })
        }
      }
    }
  }

  return { input, toolIdMap }
}

/**
 * 归一化 content 为 block 数组
 */
function normalizeContent(content) {
  if (!content) {
    return []
  }
  if (typeof content === 'string') {
    return [{ type: 'text', text: content }]
  }
  if (Array.isArray(content)) {
    return content
  }
  return []
}

/**
 * 转换 Anthropic tools → Codex tools (function 格式)
 */
function convertToolsToCodex(tools) {
  if (!Array.isArray(tools) || tools.length === 0) {
    return undefined
  }

  return tools.map((tool) => ({
    type: 'function',
    name: tool.name,
    description: tool.description || '',
    parameters: tool.input_schema || {}
  }))
}

/**
 * 转换 Anthropic tool_choice → Codex tool_choice
 */
function convertToolChoiceToCodex(toolChoice) {
  if (!toolChoice) {
    return undefined
  }

  if (typeof toolChoice === 'string') {
    if (toolChoice === 'auto') {
      return 'auto'
    }
    if (toolChoice === 'any') {
      return 'required'
    }
    if (toolChoice === 'none') {
      return 'none'
    }
    return toolChoice
  }

  if (typeof toolChoice === 'object') {
    if (toolChoice.type === 'auto') {
      return 'auto'
    }
    if (toolChoice.type === 'any') {
      return 'required'
    }
    if (toolChoice.type === 'tool' && toolChoice.name) {
      return { type: 'function', name: toolChoice.name }
    }
  }

  return undefined
}

// 已知的 Codex reasoning effort 级别
const KNOWN_REASONING_EFFORTS = new Set(['low', 'medium', 'high', 'xhigh'])

/**
 * 从模型名后缀解析 reasoning effort
 *
 * 规则：如果模型名最后一个 "-" 后的部分是已知的 effort 级别，则提取出来。
 *   gpt-5.2-medium        → { actualModel: "gpt-5.2",       reasoningEffort: "medium" }
 *   gpt-5.2-codex-xhigh   → { actualModel: "gpt-5.2-codex", reasoningEffort: "xhigh" }
 *   gpt-5.2-codex         → { actualModel: "gpt-5.2-codex", reasoningEffort: null }
 *   codex-mini-latest     → { actualModel: "codex-mini-latest", reasoningEffort: null }
 */
function parseModelWithReasoning(modelName) {
  if (!modelName || typeof modelName !== 'string') {
    return { actualModel: modelName || '', reasoningEffort: null }
  }

  const lastDash = modelName.lastIndexOf('-')
  if (lastDash <= 0) {
    return { actualModel: modelName, reasoningEffort: null }
  }

  const suffix = modelName.slice(lastDash + 1).toLowerCase()
  if (KNOWN_REASONING_EFFORTS.has(suffix)) {
    return {
      actualModel: modelName.slice(0, lastDash),
      reasoningEffort: suffix
    }
  }

  return { actualModel: modelName, reasoningEffort: null }
}

/**
 * 转换 thinking budget → Codex reasoning effort（后备逻辑）
 * 仅在模型名未指定 effort 时使用
 */
function thinkingBudgetToEffort(thinking) {
  if (!thinking || thinking.type !== 'enabled' || !thinking.budget_tokens) {
    return null
  }
  return thinking.budget_tokens <= 20000 ? 'medium' : 'high'
}

/**
 * 完整的 Anthropic → Codex 请求转换
 */
function convertAnthropicRequestToCodex(body, baseModel) {
  // 从模型名解析 reasoning effort（优先级最高）
  const { actualModel, reasoningEffort } = parseModelWithReasoning(baseModel)
  // 后备：从 thinking budget 推导 effort
  const fallbackEffort = thinkingBudgetToEffort(body.thinking)
  const effort = reasoningEffort || fallbackEffort || 'medium'

  const instructions = extractInstructionsFromSystem(body.system)
  const { input, toolIdMap } = convertMessagesToCodexInput(body.messages || [])
  const tools = convertToolsToCodex(body.tools)
  const toolChoice = convertToolChoiceToCodex(body.tool_choice)

  const codexBody = {
    model: actualModel,
    input
  }

  if (instructions) {
    codexBody.instructions = instructions
  }

  if (body.max_tokens) {
    codexBody.max_output_tokens = body.max_tokens
  }

  if (body.stream !== undefined) {
    codexBody.stream = body.stream
  }

  if (tools) {
    codexBody.tools = tools
  }

  if (toolChoice !== undefined) {
    codexBody.tool_choice = toolChoice
  }

  if (effort) {
    codexBody.reasoning = { effort, summary: 'auto' }
  }

  return { codexBody, toolIdMap, actualModel }
}

// ============================================================================
// 响应转换: Codex → Anthropic (非流式)
// ============================================================================

/**
 * 转换 Codex 非流式响应 → Anthropic Messages 格式
 */
function convertCodexResponseToAnthropic(codexResponse, baseModel, toolIdMap) {
  const content = []
  const reverseToolIdMap = new Map()
  for (const [anthropicId, codexId] of toolIdMap.entries()) {
    reverseToolIdMap.set(codexId, anthropicId)
  }

  const output = codexResponse.output || []
  for (const item of output) {
    if (item.type === 'reasoning') {
      // Combine reasoning summary text
      const summaryParts = item.summary || []
      const thinkingText = summaryParts.map((p) => p.text || '').join('')
      if (thinkingText) {
        content.push({
          type: 'thinking',
          thinking: thinkingText
        })
      }
    } else if (item.type === 'message') {
      const msgContent = item.content || []
      for (const part of msgContent) {
        if (part.type === 'output_text') {
          content.push({
            type: 'text',
            text: part.text || ''
          })
        }
      }
    } else if (item.type === 'function_call') {
      const anthropicId =
        reverseToolIdMap.get(item.call_id) || `toolu_${crypto.randomBytes(12).toString('hex')}`
      let parsedInput = {}
      if (item.arguments) {
        try {
          parsedInput = JSON.parse(item.arguments)
        } catch {
          parsedInput = { raw: item.arguments }
        }
      }
      content.push({
        type: 'tool_use',
        id: anthropicId,
        name: item.name,
        input: parsedInput
      })
    }
  }

  // Determine stop_reason
  let stopReason = 'end_turn'
  const codexStatus = codexResponse.status
  if (
    codexStatus === 'incomplete' &&
    codexResponse.incomplete_details?.reason === 'max_output_tokens'
  ) {
    stopReason = 'max_tokens'
  }
  // If there are tool_use blocks, stop_reason should be tool_use
  if (content.some((c) => c.type === 'tool_use')) {
    stopReason = 'tool_use'
  }

  // Extract usage
  const codexUsage = codexResponse.usage || {}
  const inputTokens = codexUsage.input_tokens || 0
  const outputTokens = codexUsage.output_tokens || 0
  const inputTokensDetails = codexUsage.input_tokens_details || {}
  const cacheReadInputTokens = inputTokensDetails.cached_tokens || 0

  return {
    id: `msg_${crypto.randomBytes(16).toString('hex')}`,
    type: 'message',
    role: 'assistant',
    model: CODEX_RESPONSE_MODEL_ALIAS,
    content,
    stop_reason: stopReason,
    stop_sequence: null,
    usage: {
      input_tokens: inputTokens - cacheReadInputTokens,
      output_tokens: outputTokens,
      cache_creation_input_tokens: 0,
      cache_read_input_tokens: cacheReadInputTokens
    }
  }
}

// ============================================================================
// 流式转换: Codex SSE → Anthropic SSE
// ============================================================================

/**
 * 有状态的 Codex → Anthropic 流转换器
 */
class CodexToAnthropicStreamConverter {
  constructor(res, baseModel, toolIdMap) {
    this.res = res
    this.baseModel = baseModel
    this.toolIdMap = toolIdMap
    this.reverseToolIdMap = new Map()
    this.currentBlockIndex = 0
    this.messageId = `msg_${crypto.randomBytes(16).toString('hex')}`
    this.inputTokens = 0
    this.outputTokens = 0
    this.cacheReadInputTokens = 0
    this.messageStartSent = false
    // Track current function_call item's call_id for tool_use mapping
    this.currentFunctionCallId = null
    this.currentFunctionCallName = null
  }

  /**
   * 发送 message_start 事件（只发一次）
   */
  _ensureMessageStart() {
    if (this.messageStartSent) {
      return
    }
    this.messageStartSent = true

    writeAnthropicSseEvent(this.res, 'message_start', {
      type: 'message_start',
      message: {
        id: this.messageId,
        type: 'message',
        role: 'assistant',
        model: CODEX_RESPONSE_MODEL_ALIAS,
        content: [],
        stop_reason: null,
        stop_sequence: null,
        usage: {
          input_tokens: 0,
          output_tokens: 0,
          cache_creation_input_tokens: 0,
          cache_read_input_tokens: 0
        }
      }
    })
  }

  /**
   * 处理一个 Codex SSE 事件
   */
  processEvent(event) {
    if (!event || !event.type) {
      return
    }

    switch (event.type) {
      case 'response.created':
        this._ensureMessageStart()
        break

      case 'response.output_item.added':
        this._handleOutputItemAdded(event)
        break

      case 'response.reasoning_summary_part.added':
        this._handleReasoningSummaryPartAdded()
        break

      case 'response.reasoning_summary_text.delta':
        this._handleReasoningSummaryTextDelta(event)
        break

      case 'response.reasoning_summary_part.done':
        this._handleBlockStop()
        break

      case 'response.content_part.added':
        this._handleContentPartAdded(event)
        break

      case 'response.output_text.delta':
        this._handleOutputTextDelta(event)
        break

      case 'response.content_part.done':
        this._handleBlockStop()
        break

      case 'response.function_call_arguments.delta':
        this._handleFunctionCallArgumentsDelta(event)
        break

      case 'response.output_item.done':
        this._handleOutputItemDone(event)
        break

      case 'response.completed':
        this._handleResponseCompleted(event)
        break

      default:
        // Ignore unknown events
        break
    }
  }

  _handleOutputItemAdded(event) {
    this._ensureMessageStart()
    const item = event.item || {}

    if (item.type === 'function_call') {
      // Store the call details for the content_block_start
      this.currentFunctionCallId = item.call_id
      this.currentFunctionCallName = item.name

      const anthropicId =
        this._getAnthropicToolId(item.call_id) || `toolu_${crypto.randomBytes(12).toString('hex')}`

      writeAnthropicSseEvent(this.res, 'content_block_start', {
        type: 'content_block_start',
        index: this.currentBlockIndex,
        content_block: {
          type: 'tool_use',
          id: anthropicId,
          name: item.name || '',
          input: {}
        }
      })
    }
    // For reasoning and message types, we wait for the sub-part events
  }

  _handleReasoningSummaryPartAdded() {
    this._ensureMessageStart()
    writeAnthropicSseEvent(this.res, 'content_block_start', {
      type: 'content_block_start',
      index: this.currentBlockIndex,
      content_block: {
        type: 'thinking',
        thinking: ''
      }
    })
  }

  _handleReasoningSummaryTextDelta(event) {
    const delta = event.delta || ''
    if (delta) {
      writeAnthropicSseEvent(this.res, 'content_block_delta', {
        type: 'content_block_delta',
        index: this.currentBlockIndex,
        delta: {
          type: 'thinking_delta',
          thinking: delta
        }
      })
    }
  }

  _handleContentPartAdded(event) {
    this._ensureMessageStart()
    const part = event.part || {}

    if (part.type === 'output_text') {
      writeAnthropicSseEvent(this.res, 'content_block_start', {
        type: 'content_block_start',
        index: this.currentBlockIndex,
        content_block: {
          type: 'text',
          text: ''
        }
      })
    }
  }

  _handleOutputTextDelta(event) {
    const delta = event.delta || ''
    if (delta) {
      writeAnthropicSseEvent(this.res, 'content_block_delta', {
        type: 'content_block_delta',
        index: this.currentBlockIndex,
        delta: {
          type: 'text_delta',
          text: delta
        }
      })
    }
  }

  _handleFunctionCallArgumentsDelta(event) {
    const delta = event.delta || ''
    if (delta) {
      writeAnthropicSseEvent(this.res, 'content_block_delta', {
        type: 'content_block_delta',
        index: this.currentBlockIndex,
        delta: {
          type: 'input_json_delta',
          partial_json: delta
        }
      })
    }
  }

  _handleBlockStop() {
    writeAnthropicSseEvent(this.res, 'content_block_stop', {
      type: 'content_block_stop',
      index: this.currentBlockIndex
    })
    this.currentBlockIndex++
  }

  _handleOutputItemDone(event) {
    const item = event.item || {}
    if (item.type === 'function_call') {
      // End the tool_use block
      this._handleBlockStop()
      this.currentFunctionCallId = null
      this.currentFunctionCallName = null
    }
  }

  _handleResponseCompleted(event) {
    const response = event.response || {}
    const usage = response.usage || {}
    this.inputTokens = usage.input_tokens || 0
    this.outputTokens = usage.output_tokens || 0
    const inputTokensDetails = usage.input_tokens_details || {}
    this.cacheReadInputTokens = inputTokensDetails.cached_tokens || 0

    // Determine stop reason
    let stopReason = 'end_turn'
    if (
      response.status === 'incomplete' &&
      response.incomplete_details?.reason === 'max_output_tokens'
    ) {
      stopReason = 'max_tokens'
    }
    // Check if any output items are function_call
    const hasToolUse = (response.output || []).some((item) => item.type === 'function_call')
    if (hasToolUse) {
      stopReason = 'tool_use'
    }

    // message_delta with stop_reason and usage
    writeAnthropicSseEvent(this.res, 'message_delta', {
      type: 'message_delta',
      delta: {
        stop_reason: stopReason,
        stop_sequence: null
      },
      usage: {
        input_tokens: this.inputTokens - this.cacheReadInputTokens,
        output_tokens: this.outputTokens,
        cache_read_input_tokens: this.cacheReadInputTokens,
        cache_creation_input_tokens: 0
      }
    })

    // message_stop
    writeAnthropicSseEvent(this.res, 'message_stop', {
      type: 'message_stop'
    })
  }

  _getAnthropicToolId(codexCallId) {
    for (const [anthropicId, mappedCodexId] of this.toolIdMap.entries()) {
      if (mappedCodexId === codexCallId) {
        return anthropicId
      }
    }
    return null
  }

  getUsage() {
    return {
      inputTokens: this.inputTokens - this.cacheReadInputTokens,
      outputTokens: this.outputTokens,
      cacheReadInputTokens: this.cacheReadInputTokens
    }
  }
}

// ============================================================================
// 账户选择与认证（复用 openaiRoutes 的模式）
// ============================================================================

/**
 * 选择 OpenAI 账户并获取认证信息
 */
async function selectAndAuthenticateAccount(apiKeyData, sessionHash, requestedModel) {
  const result = await unifiedOpenAIScheduler.selectAccountForApiKey(
    apiKeyData,
    sessionHash,
    requestedModel
  )

  if (!result || !result.accountId) {
    const error = new Error('No available OpenAI account found for Codex')
    error.statusCode = 402
    throw error
  }

  let account, accessToken, proxy

  if (result.accountType === 'openai-responses') {
    account = await openaiResponsesAccountService.getAccount(result.accountId)
    if (!account || !account.apiKey) {
      const error = new Error(`OpenAI-Responses account ${result.accountId} has no valid apiKey`)
      error.statusCode = 403
      throw error
    }
    accessToken = null // openai-responses 使用账户内的 apiKey

    if (account.proxy) {
      try {
        proxy = typeof account.proxy === 'string' ? JSON.parse(account.proxy) : account.proxy
      } catch (e) {
        logger.warn('Failed to parse proxy configuration:', e)
      }
    }
  } else {
    account = await openaiAccountService.getAccount(result.accountId)
    if (!account || !account.accessToken) {
      const error = new Error(`OpenAI account ${result.accountId} has no valid accessToken`)
      error.statusCode = 403
      throw error
    }

    // Token 过期自动刷新
    if (openaiAccountService.isTokenExpired(account)) {
      if (account.refreshToken) {
        logger.info(`🔄 Token expired, auto-refreshing for account ${account.name} (codex bridge)`)
        await openaiAccountService.refreshAccountToken(result.accountId)
        account = await openaiAccountService.getAccount(result.accountId)
      } else {
        const error = new Error(
          `Token expired and no refresh token available for account ${account.name}`
        )
        error.statusCode = 403
        throw error
      }
    }

    accessToken = openaiAccountService.decrypt(account.accessToken)
    if (!accessToken) {
      const error = new Error('Failed to decrypt OpenAI accessToken')
      error.statusCode = 403
      throw error
    }

    if (account.proxy) {
      try {
        proxy = typeof account.proxy === 'string' ? JSON.parse(account.proxy) : account.proxy
      } catch (e) {
        logger.warn('Failed to parse proxy configuration:', e)
      }
    }
  }

  return {
    accountId: result.accountId,
    accountType: result.accountType,
    account,
    accessToken,
    proxy
  }
}

// ============================================================================
// 主处理函数
// ============================================================================

/**
 * 处理 Anthropic Messages API 请求并桥接到 Codex
 *
 * @param {Object} req - Express request
 * @param {Object} res - Express response
 * @param {Object} options
 * @param {string} options.baseModel - 去除 vendor 前缀后的模型名
 */
async function handleAnthropicMessagesToCodex(req, res, { baseModel }) {
  let accountId = null
  let accountType = 'openai'
  let sessionHash = null

  try {
    const apiKeyData = req.apiKey || {}
    const isStream = req.body.stream === true

    // 3. 转换请求（解析 model 名中的 reasoning effort 后缀）
    const { codexBody, toolIdMap, actualModel } = convertAnthropicRequestToCodex(
      req.body,
      baseModel
    )

    logger.info(
      `🔀 [CodexBridge] Processing request: model=${actualModel}${
        actualModel !== baseModel ? ` (from ${baseModel})` : ''
      }, stream=${isStream}`
    )

    // 1. 生成会话哈希
    sessionHash = sessionHelper.generateSessionHash(req.body)

    // 2. 选择账户（使用实际模型名，不含 effort 后缀）
    const authResult = await selectAndAuthenticateAccount(apiKeyData, sessionHash, actualModel)
    ;({ accountId, accountType } = authResult)
    const { account, accessToken, proxy } = authResult

    // 4. 如果是 openai-responses 账户，直接使用 account.baseApi + /v1/responses
    //    如果是普通 openai 账户，使用 chatgpt.com/backend-api/codex/responses
    let endpoint
    const headers = {}

    if (accountType === 'openai-responses') {
      const baseApi = (account.baseApi || '').replace(/\/+$/, '')
      endpoint = `${baseApi}/v1/responses`
      // account.apiKey is already decrypted by getAccount()
      headers['authorization'] = `Bearer ${account.apiKey}`
    } else {
      endpoint = 'https://chatgpt.com/backend-api/codex/responses'
      headers['authorization'] = `Bearer ${accessToken}`
      headers['chatgpt-account-id'] =
        account.accountId || account.chatgptUserId || authResult.accountId
      headers['host'] = 'chatgpt.com'
      codexBody['store'] = false
    }

    headers['content-type'] = 'application/json'
    headers['accept'] = isStream ? 'text/event-stream' : 'application/json'

    // 5. 应用 Codex instructions 适配（与 openaiRoutes 一致）
    const userAgent = req.headers['user-agent'] || ''
    const codexCliPattern = /^(codex_vscode|codex_cli_rs|codex_exec)\/[\d.]+/i
    const isCodexCLI = codexCliPattern.test(userAgent)

    const adapterResult = adaptCodexRequestBody(codexBody, {
      isCodexCLI,
      adapterConfig: config?.openai?.codexAdapter,
      defaultInstructionsText: undefined // 让 adapter 使用配置的默认值
    })

    const finalBody = adapterResult.applied ? adapterResult.body : codexBody

    if (adapterResult.applied) {
      logger.info(
        `[CodexBridge] codexAdapter applied: instructions=${adapterResult.changes.instructions?.mode || 'none'}, stripped=${adapterResult.changes.strippedFields.length}`
      )
    }

    // 6. 构建 axios 配置
    const proxyAgent = ProxyHelper.createProxyAgent(proxy)
    const axiosConfig = {
      headers,
      timeout: config.requestTimeout || 600000,
      validateStatus: () => true
    }

    if (proxyAgent) {
      axiosConfig.httpAgent = proxyAgent
      axiosConfig.httpsAgent = proxyAgent
      axiosConfig.proxy = false
      logger.info(`🌐 [CodexBridge] Using proxy: ${ProxyHelper.getProxyDescription(proxy)}`)
    }

    // 7. 创建 AbortController 用于客户端断开时清理
    const abortController = new AbortController()
    axiosConfig.signal = abortController.signal

    const onClientClose = () => {
      logger.info(`🔌 [CodexBridge] Client disconnected, aborting upstream request`)
      abortController.abort()
    }
    req.on('close', onClientClose)

    // 8. 发送请求（Codex Responses API 仅支持 stream 模式）
    finalBody.stream = true
    const upstream = await axios.post(endpoint, finalBody, {
      ...axiosConfig,
      responseType: 'stream'
    })

    // 清理 close 监听器（已获取到响应）
    req.removeListener('close', onClientClose)

    // 9. 提取 Codex usage headers
    const codexUsageSnapshot = extractCodexUsageHeaders(upstream.headers)
    if (codexUsageSnapshot && accountType === 'openai') {
      try {
        await openaiAccountService.updateCodexUsageSnapshot(accountId, codexUsageSnapshot)
      } catch (codexError) {
        logger.error('⚠️ [CodexBridge] 更新 Codex 使用统计失败:', codexError)
      }
    }

    // 10. 处理错误状态码
    if (upstream.status === 429) {
      return await handleRateLimitError(
        upstream,
        isStream,
        accountId,
        accountType,
        sessionHash,
        res
      )
    }

    if (upstream.status === 401 || upstream.status === 402) {
      return await handleUnauthorizedError(
        upstream,
        isStream,
        accountId,
        accountType,
        sessionHash,
        res
      )
    }

    // 成功后移除限流状态
    if (upstream.status === 200 || upstream.status === 201) {
      const isRateLimited = await unifiedOpenAIScheduler.isAccountRateLimited(accountId)
      if (isRateLimited) {
        logger.info(`✅ [CodexBridge] Removing rate limit for account ${accountId} after success`)
        await unifiedOpenAIScheduler.removeAccountRateLimit(accountId, accountType)
      }
    }

    // 11. 处理响应
    if (isStream) {
      await handleStreamResponse(
        upstream,
        res,
        req,
        actualModel,
        toolIdMap,
        apiKeyData,
        accountId,
        accountType,
        abortController
      )
    } else {
      // Codex API 是 stream-only，需要先处理非 200 的流式错误
      if (upstream.status !== 200 && upstream.status !== 201) {
        const chunks = []
        await new Promise((resolve) => {
          upstream.data.on('data', (chunk) => chunks.push(chunk))
          upstream.data.on('end', resolve)
          upstream.data.on('error', resolve)
          setTimeout(resolve, 5000)
        })
        const errorBody = Buffer.concat(chunks).toString()
        logger.error(
          `❌ [CodexBridge] Non-stream error (${upstream.status}): ${errorBody.slice(0, 2000)}`
        )
        if (!res.headersSent) {
          let errorMessage = 'Codex API error'
          try {
            const parsed = JSON.parse(errorBody)
            errorMessage =
              parsed?.error?.message ||
              (typeof parsed?.error === 'string' ? parsed.error : null) ||
              parsed?.detail ||
              parsed?.message ||
              errorMessage
          } catch {
            if (errorBody) {
              errorMessage = errorBody.slice(0, 500)
            }
          }
          res.status(upstream.status).json({
            error: { type: 'api_error', message: errorMessage }
          })
        }
        return
      }

      // 收集 SSE 事件后组装为非流式响应
      const codexResponse = await collectCodexStreamAsResponse(upstream)
      if (!codexResponse) {
        if (!res.headersSent) {
          res.status(502).json({
            error: {
              type: 'api_error',
              message: 'Codex stream ended without response.completed event'
            }
          })
        }
        return
      }
      await handleNonStreamResponse(
        codexResponse,
        res,
        actualModel,
        toolIdMap,
        apiKeyData,
        accountId,
        accountType,
        req
      )
    }
  } catch (error) {
    if (error.name === 'AbortError' || error.code === 'ERR_CANCELED') {
      logger.info(`🔌 [CodexBridge] Request aborted (client disconnected)`)
      if (!res.headersSent && !res.destroyed) {
        res.status(499).end()
      }
      return
    }

    logger.error('❌ [CodexBridge] Error:', error.message)

    // Handle auth errors in catch block
    const status = error.statusCode || error.response?.status || 500
    if ((status === 401 || status === 402) && accountId) {
      try {
        await unifiedOpenAIScheduler.markAccountUnauthorized(
          accountId,
          accountType,
          sessionHash,
          `Codex bridge error: ${error.message}`
        )
      } catch (markError) {
        logger.error('❌ [CodexBridge] Failed to mark account unauthorized:', markError)
      }
    }

    if (!res.headersSent) {
      res.status(status).json({
        error: {
          type: 'error',
          message: getSafeMessage(error)
        }
      })
    } else if (!res.destroyed && !res.writableEnded) {
      res.end()
    }
  }
}

// ============================================================================
// 响应处理
// ============================================================================

/**
 * 从 Codex SSE 流中收集完整响应（用于客户端请求非流式时）
 * Codex Responses API 仅支持 stream 模式，此函数消费 SSE 事件并提取
 * response.completed 中的完整响应对象。
 */
async function collectCodexStreamAsResponse(upstream) {
  const sseParser = new IncrementalSSEParser()
  let codexResponse = null

  await new Promise((resolve) => {
    upstream.data.on('data', (chunk) => {
      try {
        const events = sseParser.feed(chunk.toString())
        for (const event of events) {
          if (event.type === 'data' && event.data?.type === 'response.completed') {
            codexResponse = event.data.response || {}
          }
        }
      } catch (err) {
        logger.error('❌ [CodexBridge] Error collecting stream event:', err)
      }
    })

    upstream.data.on('end', () => {
      const remaining = sseParser.getRemaining()
      if (remaining.trim()) {
        try {
          const events = sseParser.feed('\n\n')
          for (const event of events) {
            if (event.type === 'data' && event.data?.type === 'response.completed') {
              codexResponse = event.data.response || {}
            }
          }
        } catch {
          // ignore flush errors
        }
      }
      resolve()
    })

    upstream.data.on('error', (err) => {
      logger.error('❌ [CodexBridge] Stream error during collection:', err)
      resolve()
    })
  })

  return codexResponse
}

async function handleStreamResponse(
  upstream,
  res,
  req,
  baseModel,
  toolIdMap,
  apiKeyData,
  accountId,
  accountType,
  abortController
) {
  // 设置 SSE 响应头
  if (!res.headersSent) {
    res.setHeader('Content-Type', 'text/event-stream')
    res.setHeader('Cache-Control', 'no-cache')
    res.setHeader('Connection', 'keep-alive')
    res.setHeader('Access-Control-Allow-Origin', '*')
    res.setHeader('X-Accel-Buffering', 'no')
  }

  if (res.socket && typeof res.socket.setNoDelay === 'function') {
    res.socket.setNoDelay(true)
  }

  if (typeof res.flushHeaders === 'function') {
    res.flushHeaders()
  }

  const converter = new CodexToAnthropicStreamConverter(res, baseModel, toolIdMap)
  const sseParser = new IncrementalSSEParser()

  // 处理非 200 状态码的错误流
  if (upstream.status !== 200 && upstream.status !== 201) {
    const chunks = []
    await new Promise((resolve) => {
      upstream.data.on('data', (chunk) => chunks.push(chunk))
      upstream.data.on('end', resolve)
      upstream.data.on('error', resolve)
      setTimeout(resolve, 5000)
    })
    const errorBody = Buffer.concat(chunks).toString()
    logger.error(`❌ [CodexBridge] Upstream error (${upstream.status}): ${errorBody}`)

    writeAnthropicSseEvent(res, 'error', {
      type: 'error',
      error: {
        type: 'api_error',
        message: `Upstream Codex API error (${upstream.status})`
      }
    })
    res.end()
    return
  }

  upstream.data.on('data', (chunk) => {
    try {
      const events = sseParser.feed(chunk.toString())
      for (const event of events) {
        if (event.type === 'data' && event.data) {
          converter.processEvent(event.data)
        }
      }
    } catch (error) {
      logger.error('❌ [CodexBridge] Error processing stream chunk:', error)
    }
  })

  await new Promise((resolve) => {
    upstream.data.on('end', async () => {
      // Flush remaining buffer
      const remaining = sseParser.getRemaining()
      if (remaining.trim()) {
        const events = sseParser.feed('\n\n')
        for (const event of events) {
          if (event.type === 'data' && event.data) {
            converter.processEvent(event.data)
          }
        }
      }

      // 记录使用统计
      const usage = converter.getUsage()
      if (usage.inputTokens > 0 || usage.outputTokens > 0) {
        try {
          await apiKeyService.recordUsage(
            apiKeyData.id,
            usage.inputTokens,
            usage.outputTokens,
            0,
            usage.cacheReadInputTokens,
            baseModel,
            accountId,
            accountType
          )

          if (req.rateLimitInfo) {
            await updateRateLimitCounters(
              req.rateLimitInfo,
              {
                inputTokens: usage.inputTokens,
                outputTokens: usage.outputTokens,
                cacheCreateTokens: 0,
                cacheReadTokens: usage.cacheReadInputTokens
              },
              baseModel,
              apiKeyData.id,
              accountType
            )
          }

          logger.info(
            `📊 [CodexBridge] Stream usage recorded - Model: ${baseModel}, Input: ${usage.inputTokens}, Output: ${usage.outputTokens}, CacheRead: ${usage.cacheReadInputTokens}`
          )
        } catch (usageError) {
          logger.error('❌ [CodexBridge] Failed to record usage:', usageError)
        }
      }

      if (!res.destroyed && !res.writableEnded) {
        res.end()
      }
      resolve()
    })

    upstream.data.on('error', (err) => {
      logger.error('❌ [CodexBridge] Upstream stream error:', err)
      if (!res.destroyed && !res.writableEnded) {
        writeAnthropicSseEvent(res, 'error', {
          type: 'error',
          error: {
            type: 'api_error',
            message: 'Upstream stream error'
          }
        })
        res.end()
      }
      resolve()
    })

    // 客户端断开时清理上游流
    const cleanup = () => {
      try {
        upstream.data?.destroy?.()
        abortController?.abort?.()
      } catch (_) {
        // ignore
      }
      resolve()
    }
    req.on('close', cleanup)
    req.on('aborted', cleanup)
  })
}

async function handleNonStreamResponse(
  codexResponse,
  res,
  baseModel,
  toolIdMap,
  apiKeyData,
  accountId,
  accountType,
  req
) {
  const anthropicResponse = convertCodexResponseToAnthropic(codexResponse, baseModel, toolIdMap)

  // 记录使用统计
  const { usage } = anthropicResponse
  if (usage && (usage.input_tokens > 0 || usage.output_tokens > 0)) {
    try {
      await apiKeyService.recordUsage(
        apiKeyData.id,
        usage.input_tokens,
        usage.output_tokens,
        0,
        usage.cache_read_input_tokens || 0,
        baseModel,
        accountId,
        accountType
      )

      if (req.rateLimitInfo) {
        await updateRateLimitCounters(
          req.rateLimitInfo,
          {
            inputTokens: usage.input_tokens,
            outputTokens: usage.output_tokens,
            cacheCreateTokens: 0,
            cacheReadTokens: usage.cache_read_input_tokens || 0
          },
          baseModel,
          apiKeyData.id,
          accountType
        )
      }

      logger.info(
        `📊 [CodexBridge] Non-stream usage recorded - Model: ${baseModel}, Input: ${usage.input_tokens}, Output: ${usage.output_tokens}, CacheRead: ${usage.cache_read_input_tokens || 0}`
      )
    } catch (usageError) {
      logger.error('❌ [CodexBridge] Failed to record usage:', usageError)
    }
  }

  res.status(200).json(anthropicResponse)
}

// ============================================================================
// 错误处理
// ============================================================================

async function handleRateLimitError(upstream, isStream, accountId, accountType, sessionHash, res) {
  logger.warn(`🚫 [CodexBridge] Rate limit detected for account ${accountId}`)

  let errorData = null
  let resetsInSeconds = null

  try {
    // Codex API 始终以 stream 方式返回，需从流中读取错误体
    if (upstream.data && typeof upstream.data.on === 'function') {
      const chunks = []
      await new Promise((resolve, reject) => {
        upstream.data.on('data', (chunk) => chunks.push(chunk))
        upstream.data.on('end', resolve)
        upstream.data.on('error', reject)
        setTimeout(resolve, 5000)
      })
      const body = Buffer.concat(chunks).toString()
      logger.warn(`[CodexBridge] 429 response body: ${body.slice(0, 1000)}`)
      try {
        errorData = JSON.parse(body)
      } catch (e) {
        logger.error('[CodexBridge] Failed to parse 429 error response:', e)
      }
    } else {
      errorData = upstream.data
    }

    if (errorData?.error?.resets_in_seconds) {
      resetsInSeconds = errorData.error.resets_in_seconds
    }
  } catch (e) {
    logger.error('⚠️ [CodexBridge] Failed to parse rate limit error:', e)
  }

  await unifiedOpenAIScheduler.markAccountRateLimited(
    accountId,
    accountType,
    sessionHash,
    resetsInSeconds
  )

  if (!res.headersSent) {
    const anthropicError = {
      type: 'error',
      error: {
        type: 'rate_limit_error',
        message: errorData?.error?.message || 'Rate limit reached'
      }
    }

    if (isStream) {
      res.setHeader('Content-Type', 'text/event-stream')
      res.setHeader('Cache-Control', 'no-cache')
      writeAnthropicSseEvent(res, 'error', anthropicError)
      res.end()
    } else {
      res.status(429).json(anthropicError)
    }
  }
}

async function handleUnauthorizedError(
  upstream,
  isStream,
  accountId,
  accountType,
  sessionHash,
  res
) {
  const { status } = upstream
  logger.warn(
    `🔐 [CodexBridge] ${status === 401 ? 'Unauthorized' : 'Payment required'} for account ${accountId}`
  )

  let errorData = null
  try {
    // Codex API 始终以 stream 方式返回，需从流中读取错误体
    if (upstream.data && typeof upstream.data.on === 'function') {
      const chunks = []
      await new Promise((resolve, reject) => {
        upstream.data.on('data', (chunk) => chunks.push(chunk))
        upstream.data.on('end', resolve)
        upstream.data.on('error', reject)
        setTimeout(resolve, 5000)
      })
      const body = Buffer.concat(chunks).toString()
      logger.warn(`[CodexBridge] ${status} response body: ${body.slice(0, 1000)}`)
      try {
        errorData = JSON.parse(body)
      } catch (e) {
        errorData = { error: { message: body || 'Unauthorized' } }
      }
    } else {
      errorData = upstream.data
    }
  } catch (e) {
    logger.error(`⚠️ [CodexBridge] Failed to handle ${status} error:`, e)
  }

  const reason = `Codex bridge: ${status} error${errorData?.error?.message ? ` - ${errorData.error.message}` : ''}`

  try {
    await unifiedOpenAIScheduler.markAccountUnauthorized(
      accountId,
      accountType,
      sessionHash,
      reason
    )
  } catch (markError) {
    logger.error('❌ [CodexBridge] Failed to mark account unauthorized:', markError)
  }

  if (!res.headersSent) {
    const anthropicError = {
      type: 'error',
      error: {
        type: 'authentication_error',
        message: errorData?.error?.message || 'Authentication failed'
      }
    }

    if (isStream) {
      res.setHeader('Content-Type', 'text/event-stream')
      res.setHeader('Cache-Control', 'no-cache')
      writeAnthropicSseEvent(res, 'error', anthropicError)
      res.end()
    } else {
      res.status(status).json(anthropicError)
    }
  }
}

module.exports = {
  handleAnthropicMessagesToCodex
}
