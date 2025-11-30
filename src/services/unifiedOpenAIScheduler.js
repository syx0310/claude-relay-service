const openaiAccountService = require('./openaiAccountService')
const openaiResponsesAccountService = require('./openaiResponsesAccountService')
const accountGroupService = require('./accountGroupService')
const redis = require('../models/redis')
const logger = require('../utils/logger')
const appConfig = require('../../config/config')

class UnifiedOpenAIScheduler {
  constructor() {
    this.SESSION_MAPPING_PREFIX = 'unified_openai_session_mapping:'
  }

  // 🎛️ 获取 Codex 调度配置（带默认值）
  _getCodexSchedulingConfig() {
    const cfg = appConfig.codexScheduling || {}

    return {
      // 粘滞与新会话的额度余量阈值（百分比）
      headroomPrimaryNewPercent: cfg.headroomPrimaryNewPercent ?? 15, // 用户要求 primary 稍高
      headroomPrimaryStickyPercent: cfg.headroomPrimaryStickyPercent ?? 6,
      headroomSecondaryNewPercent: cfg.headroomSecondaryNewPercent ?? 10,
      headroomSecondaryStickyPercent: cfg.headroomSecondaryStickyPercent ?? 6,

      // 评分权重
      weightPrimaryBurn: cfg.weightPrimaryBurn ?? 1.0,
      weightSecondaryBurn: cfg.weightSecondaryBurn ?? 0.7,
      secondaryBoost: cfg.secondaryBoost ?? 20, // 放大长周期影响，避免被短窗压制
      stickinessBonus: cfg.stickinessBonus ?? 0.25,
      lowHeadroomPenalty: cfg.lowHeadroomPenalty ?? 1.2,

      // 快速过期阈值（秒），过期则鼓励刷新快要重置的配额
      minSnapshotFreshSeconds: cfg.minSnapshotFreshSeconds ?? 300,

      // 429 重试开关（暂不在此实现，保留作扩展）
      maxRetryOn429: cfg.maxRetryOn429 ?? 0
    }
  }

  // 🧮 提取 codex 使用数据
  _getCodexUsage(account) {
    return account?.codexUsage || null
  }

  // ✅ 检查账户是否有足够余量支撑新/粘滞会话
  async _hasHeadroom(account, { isSticky = false } = {}) {
    let usage = this._getCodexUsage(account)

    // 粘滞映射只有 accountId/accountType 时，补充查询一次
    if (!usage && account?.accountId && account?.accountType) {
      try {
        if (account.accountType === 'openai') {
          const full = await openaiAccountService.getAccount(account.accountId)
          usage = this._getCodexUsage(full)
        } else if (account.accountType === 'openai-responses') {
          const full = await openaiResponsesAccountService.getAccount(account.accountId)
          usage = this._getCodexUsage(full)
        }
      } catch (e) {
        logger.debug('⚠️ Failed to hydrate account for headroom check:', e.message)
      }
    }

    if (!usage) return true // 无数据则不拦截

    const cfg = this._getCodexSchedulingConfig()
    const primaryRemain =
      usage.primary?.usedPercent === null || usage.primary?.usedPercent === undefined
        ? null
        : 100 - usage.primary.usedPercent
    const secondaryRemain =
      usage.secondary?.usedPercent === null || usage.secondary?.usedPercent === undefined
        ? null
        : 100 - usage.secondary.usedPercent

    const primaryThreshold = isSticky
      ? cfg.headroomPrimaryStickyPercent
      : cfg.headroomPrimaryNewPercent
    const secondaryThreshold = isSticky
      ? cfg.headroomSecondaryStickyPercent
      : cfg.headroomSecondaryNewPercent

    if (primaryRemain !== null && primaryRemain < primaryThreshold) return false
    if (secondaryRemain !== null && secondaryRemain < secondaryThreshold) return false
    return true
  }

  // 🔢 计算账户得分：兼顾“快到重置且剩余额高”的优先级
  _computeCodexScore(account, { isSticky = false } = {}) {
    const usage = this._getCodexUsage(account)
    const cfg = this._getCodexSchedulingConfig()

    if (!usage) {
      // 无 Codex 头：优先调度一次以获取使用窗口信息
      return Number.MAX_SAFE_INTEGER
    }

    const primaryRemain =
      usage.primary?.usedPercent === null || usage.primary?.usedPercent === undefined
        ? null
        : 100 - usage.primary.usedPercent
    const secondaryRemain =
      usage.secondary?.usedPercent === null || usage.secondary?.usedPercent === undefined
        ? null
        : 100 - usage.secondary.usedPercent

    const primaryReset = usage.primary?.resetAfterSeconds
    const secondaryReset = usage.secondary?.resetAfterSeconds

    const burnPrimary =
      primaryRemain !== null && primaryReset
        ? (primaryRemain / Math.max(primaryReset, 60)) * 1000 // 放大数量级，避免惩罚掩盖收益
        : 0
    const burnSecondary =
      secondaryRemain !== null && secondaryReset
        ? (secondaryRemain / Math.max(secondaryReset, 60)) * 1000 * cfg.secondaryBoost
        : 0

    let score =
      cfg.weightPrimaryBurn * burnPrimary + cfg.weightSecondaryBurn * burnSecondary + (isSticky ? cfg.stickinessBonus : 0)

    // 低余量惩罚，防止粘滞过度占用快耗尽的号
    const primaryThreshold = isSticky
      ? cfg.headroomPrimaryStickyPercent
      : cfg.headroomPrimaryNewPercent
    const secondaryThreshold = isSticky
      ? cfg.headroomSecondaryStickyPercent
      : cfg.headroomSecondaryNewPercent

    if (primaryRemain !== null && primaryRemain < primaryThreshold) {
      score *= 0.3 // 余量临界时打折，避免强行减法导致负分
    }
    if (secondaryRemain !== null && secondaryRemain < secondaryThreshold) {
      score *= 0.6
    }

    return score
  }

  // 🔧 辅助方法：检查账户是否可调度（兼容字符串和布尔值）
  _isSchedulable(schedulable) {
    // 如果是 undefined 或 null，默认为可调度
    if (schedulable === undefined || schedulable === null) {
      return true
    }
    // 明确设置为 false（布尔值）或 'false'（字符串）时不可调度
    return schedulable !== false && schedulable !== 'false'
  }

  // 🔧 辅助方法：检查账户是否被限流（兼容字符串和对象格式）
  _isRateLimited(rateLimitStatus) {
    if (!rateLimitStatus) {
      return false
    }

    // 兼容字符串格式（Redis 原始数据）
    if (typeof rateLimitStatus === 'string') {
      return rateLimitStatus === 'limited'
    }

    // 兼容对象格式（getAllAccounts 返回的数据）
    if (typeof rateLimitStatus === 'object') {
      if (rateLimitStatus.isRateLimited === false) {
        return false
      }
      // 检查对象中的 status 字段
      return rateLimitStatus.status === 'limited' || rateLimitStatus.isRateLimited === true
    }

    return false
  }

  // 🔍 判断账号是否带有限流标记（即便已过期，用于自动恢复）
  _hasRateLimitFlag(rateLimitStatus) {
    if (!rateLimitStatus) {
      return false
    }

    if (typeof rateLimitStatus === 'string') {
      return rateLimitStatus === 'limited'
    }

    if (typeof rateLimitStatus === 'object') {
      return rateLimitStatus.status === 'limited' || rateLimitStatus.isRateLimited === true
    }

    return false
  }

  // ✅ 确保账号在调度前完成限流恢复与 schedulable 校正
  async _ensureAccountReadyForScheduling(account, accountId, { sanitized = true } = {}) {
    const hasRateLimitFlag = this._hasRateLimitFlag(account.rateLimitStatus)
    let rateLimitChecked = false
    let stillLimited = false

    let isSchedulable = this._isSchedulable(account.schedulable)

    if (!isSchedulable) {
      if (!hasRateLimitFlag) {
        return { canUse: false, reason: 'not_schedulable' }
      }

      stillLimited = await this.isAccountRateLimited(accountId)
      rateLimitChecked = true
      if (stillLimited) {
        return { canUse: false, reason: 'rate_limited' }
      }

      // 限流已恢复，矫正本地状态
      if (sanitized) {
        account.schedulable = true
      } else {
        account.schedulable = 'true'
      }
      isSchedulable = true
      logger.info(`✅ OpenAI账号 ${account.name || accountId} 已解除限流，恢复调度权限`)
    }

    if (hasRateLimitFlag) {
      if (!rateLimitChecked) {
        stillLimited = await this.isAccountRateLimited(accountId)
        rateLimitChecked = true
      }
      if (stillLimited) {
        return { canUse: false, reason: 'rate_limited' }
      }

      // 更新本地限流状态，避免重复判定
      if (sanitized) {
        account.rateLimitStatus = {
          status: 'normal',
          isRateLimited: false,
          rateLimitedAt: null,
          rateLimitResetAt: null,
          minutesRemaining: 0
        }
      } else {
        account.rateLimitStatus = 'normal'
        account.rateLimitedAt = null
        account.rateLimitResetAt = null
      }

      if (account.status === 'rateLimited') {
        account.status = 'active'
      }
    }

    if (!rateLimitChecked) {
      stillLimited = await this.isAccountRateLimited(accountId)
      if (stillLimited) {
        return { canUse: false, reason: 'rate_limited' }
      }
    }

    return { canUse: true }
  }

  // 🎯 统一调度OpenAI账号
  async selectAccountForApiKey(apiKeyData, sessionHash = null, requestedModel = null) {
    try {
      // 如果API Key绑定了专属账户或分组，优先使用
      if (apiKeyData.openaiAccountId) {
        // 检查是否是分组
        if (apiKeyData.openaiAccountId.startsWith('group:')) {
          const groupId = apiKeyData.openaiAccountId.replace('group:', '')
          logger.info(
            `🎯 API key ${apiKeyData.name} is bound to group ${groupId}, selecting from group`
          )
          return await this.selectAccountFromGroup(groupId, sessionHash, requestedModel, apiKeyData)
        }

        // 普通专属账户 - 根据前缀判断是 OpenAI 还是 OpenAI-Responses 类型
        let boundAccount = null
        let accountType = 'openai'

        // 检查是否有 responses: 前缀（用于区分 OpenAI-Responses 账户）
        if (apiKeyData.openaiAccountId.startsWith('responses:')) {
          const accountId = apiKeyData.openaiAccountId.replace('responses:', '')
          boundAccount = await openaiResponsesAccountService.getAccount(accountId)
          accountType = 'openai-responses'
        } else {
          // 普通 OpenAI 账户
          boundAccount = await openaiAccountService.getAccount(apiKeyData.openaiAccountId)
          accountType = 'openai'
        }

        const isActiveBoundAccount =
          boundAccount &&
          (boundAccount.isActive === true || boundAccount.isActive === 'true') &&
          boundAccount.status !== 'error' &&
          boundAccount.status !== 'unauthorized'

        if (isActiveBoundAccount) {
          if (accountType === 'openai') {
            const readiness = await this._ensureAccountReadyForScheduling(
              boundAccount,
              boundAccount.id,
              { sanitized: false }
            )

            if (!readiness.canUse) {
              const isRateLimited = readiness.reason === 'rate_limited'
              const errorMsg = isRateLimited
                ? `Dedicated account ${boundAccount.name} is currently rate limited`
                : `Dedicated account ${boundAccount.name} is not schedulable`
              logger.warn(`⚠️ ${errorMsg}`)
              const error = new Error(errorMsg)
              error.statusCode = isRateLimited ? 429 : 403
              throw error
            }
          } else {
            const hasRateLimitFlag = this._isRateLimited(boundAccount.rateLimitStatus)
            if (hasRateLimitFlag) {
              const isRateLimitCleared = await openaiResponsesAccountService.checkAndClearRateLimit(
                boundAccount.id
              )
              if (!isRateLimitCleared) {
                const errorMsg = `Dedicated account ${boundAccount.name} is currently rate limited`
                logger.warn(`⚠️ ${errorMsg}`)
                const error = new Error(errorMsg)
                error.statusCode = 429 // Too Many Requests - 限流
                throw error
              }
              // 限流已解除，刷新账户最新状态，确保后续调度信息准确
              boundAccount = await openaiResponsesAccountService.getAccount(boundAccount.id)
              if (!boundAccount) {
                const errorMsg = `Dedicated account ${apiKeyData.openaiAccountId} not found after rate limit reset`
                logger.warn(`⚠️ ${errorMsg}`)
                const error = new Error(errorMsg)
                error.statusCode = 404
                throw error
              }
            }

            if (!this._isSchedulable(boundAccount.schedulable)) {
              const errorMsg = `Dedicated account ${boundAccount.name} is not schedulable`
              logger.warn(`⚠️ ${errorMsg}`)
              const error = new Error(errorMsg)
              error.statusCode = 403 // Forbidden - 调度被禁止
              throw error
            }

            // ⏰ 检查 OpenAI-Responses 专属账户订阅是否过期
            if (openaiResponsesAccountService.isSubscriptionExpired(boundAccount)) {
              const errorMsg = `Dedicated account ${boundAccount.name} subscription has expired`
              logger.warn(`⚠️ ${errorMsg}`)
              const error = new Error(errorMsg)
              error.statusCode = 403 // Forbidden - 订阅已过期
              throw error
            }
          }

          // 专属账户：可选的模型检查（只有明确配置了supportedModels且不为空才检查）
          // OpenAI-Responses 账户默认支持所有模型
          if (
            accountType === 'openai' &&
            requestedModel &&
            boundAccount.supportedModels &&
            boundAccount.supportedModels.length > 0
          ) {
            const modelSupported = boundAccount.supportedModels.includes(requestedModel)
            if (!modelSupported) {
              const errorMsg = `Dedicated account ${boundAccount.name} does not support model ${requestedModel}`
              logger.warn(`⚠️ ${errorMsg}`)
              const error = new Error(errorMsg)
              error.statusCode = 400 // Bad Request - 请求参数错误
              throw error
            }
          }

          logger.info(
            `🎯 Using bound dedicated ${accountType} account: ${boundAccount.name} (${boundAccount.id}) for API key ${apiKeyData.name}`
          )
          // 更新账户的最后使用时间
          if (accountType === 'openai') {
            await openaiAccountService.recordUsage(boundAccount.id, 0)
          } else {
            await openaiResponsesAccountService.updateAccount(boundAccount.id, {
              lastUsedAt: new Date().toISOString()
            })
          }
          return {
            accountId: boundAccount.id,
            accountType
          }
        } else {
          // 专属账户不可用时直接报错，不降级到共享池
          let errorMsg
          if (!boundAccount) {
            errorMsg = `Dedicated account ${apiKeyData.openaiAccountId} not found`
          } else if (!(boundAccount.isActive === true || boundAccount.isActive === 'true')) {
            errorMsg = `Dedicated account ${boundAccount.name} is not active`
          } else if (boundAccount.status === 'unauthorized') {
            errorMsg = `Dedicated account ${boundAccount.name} is unauthorized`
          } else if (boundAccount.status === 'error') {
            errorMsg = `Dedicated account ${boundAccount.name} is not available (error status)`
          } else {
            errorMsg = `Dedicated account ${boundAccount.name} is not available (inactive or forbidden)`
          }
          logger.warn(`⚠️ ${errorMsg}`)
          const error = new Error(errorMsg)
          error.statusCode = boundAccount ? 403 : 404 // Forbidden 或 Not Found
          throw error
        }
      }

      // 如果有会话哈希，检查是否有已映射的账户
      if (sessionHash) {
        const mappedAccount = await this._getSessionMapping(sessionHash)
        if (mappedAccount) {
          // 验证映射的账户是否仍然可用
          const isAvailable = await this._isAccountAvailable(
            mappedAccount.accountId,
            mappedAccount.accountType
          )
          const hasHeadroom = await this._hasHeadroom(mappedAccount, { isSticky: true })
          if (isAvailable && hasHeadroom) {
            // 🚀 智能会话续期（续期 unified 映射键，按配置）
            await this._extendSessionMappingTTL(sessionHash)
            logger.info(
              `🎯 Using sticky session account: ${mappedAccount.accountId} (${mappedAccount.accountType}) for session ${sessionHash}`
            )
            // 更新账户的最后使用时间
            await openaiAccountService.recordUsage(mappedAccount.accountId, 0)
            return mappedAccount
          } else {
            logger.warn(
              `⚠️ Mapped account ${mappedAccount.accountId} dropped (available: ${isAvailable}, headroom: ${hasHeadroom}), selecting new account`
            )
            await this._deleteSessionMapping(sessionHash)
          }
        }
      }

      // 获取所有可用账户
      const availableAccounts = await this._getAllAvailableAccounts(apiKeyData, requestedModel)

      if (availableAccounts.length === 0) {
        // 提供更详细的错误信息
        if (requestedModel) {
          const error = new Error(
            `No available OpenAI accounts support the requested model: ${requestedModel}`
          )
          error.statusCode = 400 // Bad Request - 模型不支持
          throw error
        } else {
          const error = new Error('No available OpenAI accounts')
          error.statusCode = 402 // Payment Required - 资源耗尽
          throw error
        }
      }

      // 先过滤余量不足的号（有数据才拦）
      const headroomCandidates = []
      for (const acc of availableAccounts) {
        if (await this._hasHeadroom(acc, { isSticky: false })) {
          headroomCandidates.push(acc)
        }
      }
      if (headroomCandidates.length === 0 && availableAccounts.length > 0) {
        logger.warn(
          `⚠️ All ${availableAccounts.length} accounts have insufficient headroom, using fallback pool`
        )
      }
      const candidatePool = headroomCandidates.length > 0 ? headroomCandidates : availableAccounts

      // 评分排序：优先消耗即将重置的周/日额度，其次最久未用
      const sortedAccounts = candidatePool.sort((a, b) => {
        const scoreA = this._computeCodexScore(a, { isSticky: false })
        const scoreB = this._computeCodexScore(b, { isSticky: false })

        const bothFinite = Number.isFinite(scoreA) && Number.isFinite(scoreB)
        if (bothFinite && scoreA !== scoreB) return scoreB - scoreA // 分高优先

        if (bothFinite && scoreA === scoreB) {
          // tie-break by lastUsedAt
          const aLastUsed = new Date(a.lastUsedAt || 0).getTime()
          const bLastUsed = new Date(b.lastUsedAt || 0).getTime()
          return aLastUsed - bLastUsed
        }

        if (Number.isFinite(scoreA)) return -1
        if (Number.isFinite(scoreB)) return 1

        const aLastUsed = new Date(a.lastUsedAt || 0).getTime()
        const bLastUsed = new Date(b.lastUsedAt || 0).getTime()
        return aLastUsed - bLastUsed
      })

      // 选择得分最高的账户
      const selectedAccount = sortedAccounts[0]

      // 如果有会话哈希，建立新的映射
      if (sessionHash) {
        await this._setSessionMapping(
          sessionHash,
          selectedAccount.accountId,
          selectedAccount.accountType
        )
        logger.info(
          `🎯 Created new sticky session mapping: ${selectedAccount.name} (${selectedAccount.accountId}, ${selectedAccount.accountType}) for session ${sessionHash}`
        )
      }

      logger.info(
        `🎯 Selected account: ${selectedAccount.name} (${selectedAccount.accountId}, ${selectedAccount.accountType}) for API key ${apiKeyData.name}`
      )

      // 更新账户的最后使用时间
      await openaiAccountService.recordUsage(selectedAccount.accountId, 0)

      return {
        accountId: selectedAccount.accountId,
        accountType: selectedAccount.accountType
      }
    } catch (error) {
      logger.error('❌ Failed to select account for API key:', error)
      throw error
    }
  }

  // 📋 获取所有可用账户（仅共享池）
  async _getAllAvailableAccounts(apiKeyData, requestedModel = null) {
    const availableAccounts = []

    // 注意：专属账户的处理已经在 selectAccountForApiKey 中完成
    // 这里只处理共享池账户

    // 获取所有OpenAI账户（共享池）
    const openaiAccounts = await openaiAccountService.getAllAccounts()
    for (let account of openaiAccounts) {
      if (
        account.isActive &&
        account.status !== 'error' &&
        (account.accountType === 'shared' || !account.accountType) // 兼容旧数据
      ) {
        const accountId = account.id || account.accountId

        const readiness = await this._ensureAccountReadyForScheduling(account, accountId, {
          sanitized: true
        })

        if (!readiness.canUse) {
          if (readiness.reason === 'rate_limited') {
            logger.debug(`⏭️ 跳过 OpenAI 账号 ${account.name} - 仍处于限流状态`)
          } else {
            logger.debug(`⏭️ 跳过 OpenAI 账号 ${account.name} - 已被管理员禁用调度`)
          }
          continue
        }

        // 检查token是否过期并自动刷新
        const isExpired = openaiAccountService.isTokenExpired(account)
        if (isExpired) {
          if (!account.refreshToken) {
            logger.warn(
              `⚠️ OpenAI account ${account.name} token expired and no refresh token available`
            )
            continue
          }

          // 自动刷新过期的 token
          try {
            logger.info(`🔄 Auto-refreshing expired token for OpenAI account ${account.name}`)
            await openaiAccountService.refreshAccountToken(account.id)
            // 重新获取更新后的账户信息
            account = await openaiAccountService.getAccount(account.id)
            logger.info(`✅ Token refreshed successfully for ${account.name}`)
          } catch (refreshError) {
            logger.error(`❌ Failed to refresh token for ${account.name}:`, refreshError.message)
            continue // 刷新失败，跳过此账户
          }
        }

        // 检查模型支持（仅在明确设置了supportedModels且不为空时才检查）
        // 如果没有设置supportedModels或为空数组，则支持所有模型
        if (requestedModel && account.supportedModels && account.supportedModels.length > 0) {
          const modelSupported = account.supportedModels.includes(requestedModel)
          if (!modelSupported) {
            logger.debug(
              `⏭️ Skipping OpenAI account ${account.name} - doesn't support model ${requestedModel}`
            )
            continue
          }
        }

        availableAccounts.push({
          ...account,
          accountId: account.id,
          accountType: 'openai',
          priority: parseInt(account.priority) || 50,
          lastUsedAt: account.lastUsedAt || '0'
        })
      }
    }

    // 获取所有 OpenAI-Responses 账户（共享池）
    const openaiResponsesAccounts = await openaiResponsesAccountService.getAllAccounts()
    for (const account of openaiResponsesAccounts) {
      if (
        (account.isActive === true || account.isActive === 'true') &&
        account.status !== 'error' &&
        account.status !== 'rateLimited' &&
        (account.accountType === 'shared' || !account.accountType)
      ) {
        const hasRateLimitFlag = this._hasRateLimitFlag(account.rateLimitStatus)
        const schedulable = this._isSchedulable(account.schedulable)

        if (!schedulable && !hasRateLimitFlag) {
          logger.debug(`⏭️ Skipping OpenAI-Responses account ${account.name} - not schedulable`)
          continue
        }

        let isRateLimitCleared = false
        if (hasRateLimitFlag) {
          isRateLimitCleared = await openaiResponsesAccountService.checkAndClearRateLimit(
            account.id
          )

          if (!isRateLimitCleared) {
            logger.debug(`⏭️ Skipping OpenAI-Responses account ${account.name} - rate limited`)
            continue
          }

          if (!schedulable) {
            account.schedulable = 'true'
            account.status = 'active'
            logger.info(`✅ OpenAI-Responses账号 ${account.name} 已解除限流，恢复调度权限`)
          }
        }

        // ⏰ 检查订阅是否过期
        if (openaiResponsesAccountService.isSubscriptionExpired(account)) {
          logger.debug(
            `⏭️ Skipping OpenAI-Responses account ${account.name} - subscription expired`
          )
          continue
        }

        // OpenAI-Responses 账户默认支持所有模型
        // 因为它们是第三方兼容 API，模型支持由第三方决定

        availableAccounts.push({
          ...account,
          accountId: account.id,
          accountType: 'openai-responses',
          priority: parseInt(account.priority) || 50,
          lastUsedAt: account.lastUsedAt || '0'
        })
      }
    }

    return availableAccounts
  }

  // 🔢 按优先级和最后使用时间排序账户（已废弃，改为与 Claude 保持一致，只按最后使用时间排序）
  // _sortAccountsByPriority(accounts) {
  //   return accounts.sort((a, b) => {
  //     // 首先按优先级排序（数字越小优先级越高）
  //     if (a.priority !== b.priority) {
  //       return a.priority - b.priority
  //     }

  //     // 优先级相同时，按最后使用时间排序（最久未使用的优先）
  //     const aLastUsed = new Date(a.lastUsedAt || 0).getTime()
  //     const bLastUsed = new Date(b.lastUsedAt || 0).getTime()
  //     return aLastUsed - bLastUsed
  //   })
  // }

  // 🔍 检查账户是否可用
  async _isAccountAvailable(accountId, accountType) {
    try {
      if (accountType === 'openai') {
        const account = await openaiAccountService.getAccount(accountId)
        if (
          !account ||
          !account.isActive ||
          account.status === 'error' ||
          account.status === 'unauthorized'
        ) {
          return false
        }
        const readiness = await this._ensureAccountReadyForScheduling(account, accountId, {
          sanitized: false
        })

        if (!readiness.canUse) {
          if (readiness.reason === 'rate_limited') {
            logger.debug(
              `🚫 OpenAI account ${accountId} still rate limited when checking availability`
            )
          } else {
            logger.info(`🚫 OpenAI account ${accountId} is not schedulable`)
          }
          return false
        }

        return true
      } else if (accountType === 'openai-responses') {
        const account = await openaiResponsesAccountService.getAccount(accountId)
        if (
          !account ||
          (account.isActive !== true && account.isActive !== 'true') ||
          account.status === 'error' ||
          account.status === 'unauthorized'
        ) {
          return false
        }
        // 检查是否可调度
        if (!this._isSchedulable(account.schedulable)) {
          logger.info(`🚫 OpenAI-Responses account ${accountId} is not schedulable`)
          return false
        }
        // ⏰ 检查订阅是否过期
        if (openaiResponsesAccountService.isSubscriptionExpired(account)) {
          logger.info(`🚫 OpenAI-Responses account ${accountId} subscription expired`)
          return false
        }
        // 检查并清除过期的限流状态
        const isRateLimitCleared =
          await openaiResponsesAccountService.checkAndClearRateLimit(accountId)
        return !this._isRateLimited(account.rateLimitStatus) || isRateLimitCleared
      }
      return false
    } catch (error) {
      logger.warn(`⚠️ Failed to check account availability: ${accountId}`, error)
      return false
    }
  }

  // 🔗 获取会话映射
  async _getSessionMapping(sessionHash) {
    const client = redis.getClientSafe()
    const mappingData = await client.get(`${this.SESSION_MAPPING_PREFIX}${sessionHash}`)

    if (mappingData) {
      try {
        return JSON.parse(mappingData)
      } catch (error) {
        logger.warn('⚠️ Failed to parse session mapping:', error)
        return null
      }
    }

    return null
  }

  // 💾 设置会话映射
  async _setSessionMapping(sessionHash, accountId, accountType) {
    const client = redis.getClientSafe()
    const mappingData = JSON.stringify({ accountId, accountType })
    // 依据配置设置TTL（小时）
    const appConfig = require('../../config/config')
    const ttlHours = appConfig.session?.stickyTtlHours || 1
    const ttlSeconds = Math.max(1, Math.floor(ttlHours * 60 * 60))
    await client.setex(`${this.SESSION_MAPPING_PREFIX}${sessionHash}`, ttlSeconds, mappingData)
  }

  // 🗑️ 删除会话映射
  async _deleteSessionMapping(sessionHash) {
    const client = redis.getClientSafe()
    await client.del(`${this.SESSION_MAPPING_PREFIX}${sessionHash}`)
  }

  // 🔁 续期统一调度会话映射TTL（针对 unified_openai_session_mapping:* 键），遵循会话配置
  async _extendSessionMappingTTL(sessionHash) {
    try {
      const client = redis.getClientSafe()
      const key = `${this.SESSION_MAPPING_PREFIX}${sessionHash}`
      const remainingTTL = await client.ttl(key)

      if (remainingTTL === -2) {
        return false
      }
      if (remainingTTL === -1) {
        return true
      }

      const appConfig = require('../../config/config')
      const ttlHours = appConfig.session?.stickyTtlHours || 1
      const renewalThresholdMinutes = appConfig.session?.renewalThresholdMinutes || 0
      if (!renewalThresholdMinutes) {
        return true
      }

      const fullTTL = Math.max(1, Math.floor(ttlHours * 60 * 60))
      const threshold = Math.max(0, Math.floor(renewalThresholdMinutes * 60))

      if (remainingTTL < threshold) {
        await client.expire(key, fullTTL)
        logger.debug(
          `🔄 Renewed unified OpenAI session TTL: ${sessionHash} (was ${Math.round(remainingTTL / 60)}m, renewed to ${ttlHours}h)`
        )
      } else {
        logger.debug(
          `✅ Unified OpenAI session TTL sufficient: ${sessionHash} (remaining ${Math.round(remainingTTL / 60)}m)`
        )
      }
      return true
    } catch (error) {
      logger.error('❌ Failed to extend unified OpenAI session TTL:', error)
      return false
    }
  }

  // 🚫 标记账户为限流状态
  async markAccountRateLimited(accountId, accountType, sessionHash = null, resetsInSeconds = null) {
    try {
      if (accountType === 'openai') {
        await openaiAccountService.setAccountRateLimited(accountId, true, resetsInSeconds)
      } else if (accountType === 'openai-responses') {
        // 对于 OpenAI-Responses 账户，使用与普通 OpenAI 账户类似的处理方式
        const duration = resetsInSeconds ? Math.ceil(resetsInSeconds / 60) : null
        await openaiResponsesAccountService.markAccountRateLimited(accountId, duration)

        // 同时更新调度状态，避免继续被调度
        await openaiResponsesAccountService.updateAccount(accountId, {
          schedulable: 'false',
          rateLimitResetAt: resetsInSeconds
            ? new Date(Date.now() + resetsInSeconds * 1000).toISOString()
            : new Date(Date.now() + 3600000).toISOString() // 默认1小时
        })
      }

      // 删除会话映射
      if (sessionHash) {
        await this._deleteSessionMapping(sessionHash)
      }

      return { success: true }
    } catch (error) {
      logger.error(
        `❌ Failed to mark account as rate limited: ${accountId} (${accountType})`,
        error
      )
      throw error
    }
  }

  // 🚫 标记账户为未授权状态
  async markAccountUnauthorized(
    accountId,
    accountType,
    sessionHash = null,
    reason = 'OpenAI账号认证失败（401错误）'
  ) {
    try {
      if (accountType === 'openai') {
        await openaiAccountService.markAccountUnauthorized(accountId, reason)
      } else if (accountType === 'openai-responses') {
        await openaiResponsesAccountService.markAccountUnauthorized(accountId, reason)
      } else {
        logger.warn(
          `⚠️ Unsupported account type ${accountType} when marking unauthorized for account ${accountId}`
        )
        return { success: false }
      }

      if (sessionHash) {
        await this._deleteSessionMapping(sessionHash)
      }

      return { success: true }
    } catch (error) {
      logger.error(
        `❌ Failed to mark account as unauthorized: ${accountId} (${accountType})`,
        error
      )
      throw error
    }
  }

  // ✅ 移除账户的限流状态
  async removeAccountRateLimit(accountId, accountType) {
    try {
      if (accountType === 'openai') {
        await openaiAccountService.setAccountRateLimited(accountId, false)
      } else if (accountType === 'openai-responses') {
        // 清除 OpenAI-Responses 账户的限流状态
        await openaiResponsesAccountService.updateAccount(accountId, {
          rateLimitedAt: '',
          rateLimitStatus: '',
          rateLimitResetAt: '',
          status: 'active',
          errorMessage: '',
          schedulable: 'true'
        })
        logger.info(`✅ Rate limit cleared for OpenAI-Responses account ${accountId}`)
      }

      return { success: true }
    } catch (error) {
      logger.error(
        `❌ Failed to remove rate limit for account: ${accountId} (${accountType})`,
        error
      )
      throw error
    }
  }

  // 🔍 检查账户是否处于限流状态
  async isAccountRateLimited(accountId) {
    try {
      const account = await openaiAccountService.getAccount(accountId)
      if (!account) {
        return false
      }

      if (this._isRateLimited(account.rateLimitStatus)) {
        // 如果有具体的重置时间，使用它
        if (account.rateLimitResetAt) {
          const resetTime = new Date(account.rateLimitResetAt).getTime()
          const now = Date.now()
          const isStillLimited = now < resetTime

          // 如果已经过了重置时间，自动清除限流状态
          if (!isStillLimited) {
            logger.info(`✅ Auto-clearing rate limit for account ${accountId} (reset time reached)`)
            await openaiAccountService.setAccountRateLimited(accountId, false)
            return false
          }

          return isStillLimited
        }

        // 如果没有具体的重置时间，使用默认的1小时
        if (account.rateLimitedAt) {
          const limitedAt = new Date(account.rateLimitedAt).getTime()
          const now = Date.now()
          const limitDuration = 60 * 60 * 1000 // 1小时
          return now < limitedAt + limitDuration
        }
      }
      return false
    } catch (error) {
      logger.error(`❌ Failed to check rate limit status: ${accountId}`, error)
      return false
    }
  }

  // 👥 从分组中选择账户
  async selectAccountFromGroup(groupId, sessionHash = null, requestedModel = null) {
    try {
      // 获取分组信息
      const group = await accountGroupService.getGroup(groupId)
      if (!group) {
        const error = new Error(`Group ${groupId} not found`)
        error.statusCode = 404 // Not Found - 资源不存在
        throw error
      }

      if (group.platform !== 'openai') {
        const error = new Error(`Group ${group.name} is not an OpenAI group`)
        error.statusCode = 400 // Bad Request - 请求参数错误
        throw error
      }

      logger.info(`👥 Selecting account from OpenAI group: ${group.name}`)

      // 如果有会话哈希，检查是否有已映射的账户
      if (sessionHash) {
        const mappedAccount = await this._getSessionMapping(sessionHash)
        if (mappedAccount) {
          // 验证映射的账户是否仍然可用并且在分组中
          const isInGroup = await this._isAccountInGroup(mappedAccount.accountId, groupId)
          if (isInGroup) {
            const isAvailable = await this._isAccountAvailable(
              mappedAccount.accountId,
              mappedAccount.accountType
            )
            if (isAvailable) {
              // 🚀 智能会话续期（续期 unified 映射键，按配置）
              await this._extendSessionMappingTTL(sessionHash)
              logger.info(
                `🎯 Using sticky session account from group: ${mappedAccount.accountId} (${mappedAccount.accountType})`
              )
              // 更新账户的最后使用时间
              await openaiAccountService.recordUsage(mappedAccount.accountId, 0)
              return mappedAccount
            }
          }
          // 如果账户不可用或不在分组中，删除映射
          await this._deleteSessionMapping(sessionHash)
        }
      }

      // 获取分组成员
      const memberIds = await accountGroupService.getGroupMembers(groupId)
      if (memberIds.length === 0) {
        const error = new Error(`Group ${group.name} has no members`)
        error.statusCode = 402 // Payment Required - 资源耗尽
        throw error
      }

      // 获取可用的分组成员账户（支持 OpenAI 和 OpenAI-Responses 两种类型）
      const availableAccounts = []
      for (const memberId of memberIds) {
        // 首先尝试从 OpenAI 账户服务获取
        let account = await openaiAccountService.getAccount(memberId)
        let accountType = 'openai'

        // 如果 OpenAI 账户不存在，尝试从 OpenAI-Responses 账户服务获取
        if (!account) {
          account = await openaiResponsesAccountService.getAccount(memberId)
          accountType = 'openai-responses'
        }

        if (
          account &&
          (account.isActive === true || account.isActive === 'true') &&
          account.status !== 'error'
        ) {
          const readiness = await this._ensureAccountReadyForScheduling(account, account.id, {
            sanitized: false
          })

          if (!readiness.canUse) {
            if (readiness.reason === 'rate_limited') {
              logger.debug(
                `⏭️ Skipping group member ${accountType} account ${account.name} - still rate limited`
              )
            } else {
              logger.debug(
                `⏭️ Skipping group member ${accountType} account ${account.name} - not schedulable`
              )
            }
            continue
          }

          // 检查token是否过期（仅对 OpenAI OAuth 账户检查）
          if (accountType === 'openai') {
            const isExpired = openaiAccountService.isTokenExpired(account)
            if (isExpired && !account.refreshToken) {
              logger.warn(
                `⚠️ Group member OpenAI account ${account.name} token expired and no refresh token available`
              )
              continue
            }
          }

          // 检查模型支持（仅在明确设置了supportedModels且不为空时才检查）
          // 如果没有设置supportedModels或为空数组，则支持所有模型
          if (requestedModel && account.supportedModels && account.supportedModels.length > 0) {
            const modelSupported = account.supportedModels.includes(requestedModel)
            if (!modelSupported) {
              logger.debug(
                `⏭️ Skipping group member ${accountType} account ${account.name} - doesn't support model ${requestedModel}`
              )
              continue
            }
          }

          // 添加到可用账户列表
          availableAccounts.push({
            ...account,
            accountId: account.id,
            accountType,
            priority: parseInt(account.priority) || 50,
            lastUsedAt: account.lastUsedAt || '0'
          })
        }
      }

      if (availableAccounts.length === 0) {
        const error = new Error(`No available accounts in group ${group.name}`)
        error.statusCode = 402 // Payment Required - 资源耗尽
        throw error
      }

      // 使用 Codex 余量与评分算法挑选分组账户
      const headroomAccounts = []
      for (const acc of availableAccounts) {
        if (await this._hasHeadroom(acc, { isSticky: false })) {
          headroomAccounts.push(acc)
        }
      }

      if (headroomAccounts.length === 0 && availableAccounts.length > 0) {
        logger.warn(
          `⚠️ Group ${group.name} has ${availableAccounts.length} members but none meet headroom, using fallback`
        )
      }

      const candidatePool = headroomAccounts.length > 0 ? headroomAccounts : availableAccounts

      const sortedAccounts = candidatePool.sort((a, b) => {
        const scoreA = this._computeCodexScore(a, { isSticky: false })
        const scoreB = this._computeCodexScore(b, { isSticky: false })

        const bothFinite = Number.isFinite(scoreA) && Number.isFinite(scoreB)
        if (bothFinite && scoreA !== scoreB) return scoreB - scoreA

        if (bothFinite && scoreA === scoreB) {
          const aLastUsed = new Date(a.lastUsedAt || 0).getTime()
          const bLastUsed = new Date(b.lastUsedAt || 0).getTime()
          return aLastUsed - bLastUsed
        }

        if (Number.isFinite(scoreA)) return -1
        if (Number.isFinite(scoreB)) return 1

        const aLastUsed = new Date(a.lastUsedAt || 0).getTime()
        const bLastUsed = new Date(b.lastUsedAt || 0).getTime()
        return aLastUsed - bLastUsed
      })

      // 选择得分最高的账户
      const selectedAccount = sortedAccounts[0]

      // 如果有会话哈希，建立新的映射
      if (sessionHash) {
        await this._setSessionMapping(
          sessionHash,
          selectedAccount.accountId,
          selectedAccount.accountType
        )
        logger.info(
          `🎯 Created new sticky session mapping from group: ${selectedAccount.name} (${selectedAccount.accountId})`
        )
      }

      logger.info(
        `🎯 Selected account from group: ${selectedAccount.name} (${selectedAccount.accountId})`
      )

      // 更新账户的最后使用时间
      await openaiAccountService.recordUsage(selectedAccount.accountId, 0)

      return {
        accountId: selectedAccount.accountId,
        accountType: selectedAccount.accountType
      }
    } catch (error) {
      logger.error(`❌ Failed to select account from group ${groupId}:`, error)
      throw error
    }
  }

  // 🔍 检查账户是否在分组中
  async _isAccountInGroup(accountId, groupId) {
    const members = await accountGroupService.getGroupMembers(groupId)
    return members.includes(accountId)
  }

  // 📊 更新账户最后使用时间
  async updateAccountLastUsed(accountId, accountType) {
    try {
      if (accountType === 'openai') {
        await openaiAccountService.updateAccount(accountId, {
          lastUsedAt: new Date().toISOString()
        })
      }
    } catch (error) {
      logger.warn(`⚠️ Failed to update last used time for account ${accountId}:`, error)
    }
  }
}

module.exports = new UnifiedOpenAIScheduler()
