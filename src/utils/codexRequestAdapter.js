const DEFAULT_NON_CODEX_FIELDS_TO_REMOVE = [
  'temperature',
  'top_p',
  'max_output_tokens',
  'user',
  'text_formatting',
  'truncation',
  'text',
  'service_tier',
  'prompt_cache_retention',
  'safety_identifier'
]

function isNonEmptyString(value) {
  return typeof value === 'string' && value.trim()
}

function normalizeInstructionsMode(value) {
  const mode = typeof value === 'string' ? value.toLowerCase() : ''
  if (mode === 'prepend' || mode === 'overwrite' || mode === 'none') {
    return mode
  }
  return 'overwrite'
}

function normalizeApplyWhen(value) {
  const when = typeof value === 'string' ? value.toLowerCase() : ''
  if (when === 'all') {
    return 'all'
  }
  return 'non_codex'
}

function resolveAdapterConfig(rawConfig, defaultInstructionsText) {
  const safe = rawConfig && typeof rawConfig === 'object' ? rawConfig : {}

  const enabled = safe.enabled !== false

  const instructionsRaw =
    safe.instructions && typeof safe.instructions === 'object' ? safe.instructions : {}
  const stripFieldsRaw =
    safe.stripFields && typeof safe.stripFields === 'object' ? safe.stripFields : {}

  const instructionsMode = normalizeInstructionsMode(instructionsRaw.mode)
  const instructionsApplyWhen = normalizeApplyWhen(instructionsRaw.applyWhen || safe.applyWhen)
  const instructionsText = isNonEmptyString(instructionsRaw.text)
    ? instructionsRaw.text
    : defaultInstructionsText

  const stripFieldsEnabled = stripFieldsRaw.enabled !== false
  const stripFieldsFields =
    Array.isArray(stripFieldsRaw.fields) && stripFieldsRaw.fields.length > 0
      ? stripFieldsRaw.fields
      : DEFAULT_NON_CODEX_FIELDS_TO_REMOVE

  return {
    enabled,
    instructions: { mode: instructionsMode, applyWhen: instructionsApplyWhen, text: instructionsText },
    stripFields: { enabled: stripFieldsEnabled, fields: stripFieldsFields }
  }
}

/**
 * Applies Codex compatibility adaptations to an OpenAI Responses request body.
 *
 * - Strips unsupported/irrelevant fields (non-Codex clients only)
 * - Injects/overrides instructions based on configured mode
 *
 * @param {Object} body
 * @param {Object} options
 * @param {boolean} options.isCodexCLI
 * @param {Object} options.adapterConfig
 * @param {string} options.defaultInstructionsText
 * @returns {{body: Object, applied: boolean, changes: {strippedFields: string[], instructions: null | {mode: string, alreadyPresent?: boolean, clientMissing?: boolean}}}}
 */
function adaptCodexRequestBody(body, { isCodexCLI, adapterConfig, defaultInstructionsText }) {
  if (!body || typeof body !== 'object') {
    return { body, applied: false, changes: { strippedFields: [], instructions: null } }
  }

  const settings = resolveAdapterConfig(adapterConfig, defaultInstructionsText)
  const changes = { strippedFields: [], instructions: null }

  if (!settings.enabled) {
    return { body, applied: false, changes }
  }

  const processedBody = { ...body }

  // Field stripping stays scoped to non-Codex clients to avoid breaking Codex CLI.
  if (!isCodexCLI && settings.stripFields.enabled) {
    for (const field of settings.stripFields.fields) {
      if (Object.prototype.hasOwnProperty.call(processedBody, field)) {
        delete processedBody[field]
        changes.strippedFields.push(field)
      }
    }
  }

  const shouldApplyInstructions =
    settings.instructions.mode !== 'none' &&
    isNonEmptyString(settings.instructions.text) &&
    (settings.instructions.applyWhen === 'all' || !isCodexCLI)

  if (shouldApplyInstructions) {
    if (settings.instructions.mode === 'overwrite') {
      processedBody.instructions = settings.instructions.text
      changes.instructions = { mode: 'overwrite' }
    } else if (settings.instructions.mode === 'prepend') {
      const clientText = typeof body.instructions === 'string' ? body.instructions : ''
      const alreadyPresent =
        clientText.startsWith(settings.instructions.text) ||
        clientText.trimStart().startsWith(settings.instructions.text)

      if (clientText && alreadyPresent) {
        processedBody.instructions = body.instructions
        changes.instructions = { mode: 'prepend', alreadyPresent: true }
      } else if (clientText && clientText.trim()) {
        processedBody.instructions = `${settings.instructions.text}\n\n${clientText}`
        changes.instructions = { mode: 'prepend', alreadyPresent: false }
      } else {
        processedBody.instructions = settings.instructions.text
        changes.instructions = { mode: 'prepend', alreadyPresent: false, clientMissing: true }
      }
    }
  }

  const applied = changes.strippedFields.length > 0 || changes.instructions !== null
  return { body: processedBody, applied, changes }
}

module.exports = {
  DEFAULT_NON_CODEX_FIELDS_TO_REMOVE,
  adaptCodexRequestBody
}

