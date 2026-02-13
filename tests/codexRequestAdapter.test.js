const { adaptCodexRequestBody } = require('../src/utils/codexRequestAdapter')

describe('codexRequestAdapter', () => {
  it('overwrite: should override instructions and strip non-codex fields', () => {
    const input = { instructions: 'CLIENT', temperature: 1, foo: 'bar' }

    const result = adaptCodexRequestBody(input, {
      isCodexCLI: false,
      adapterConfig: {
        instructions: { mode: 'overwrite', text: 'SERVER' },
        stripFields: { enabled: true }
      },
      defaultInstructionsText: 'DEFAULT'
    })

    expect(result.applied).toBe(true)
    expect(result.body.instructions).toBe('SERVER')
    expect(result.body.foo).toBe('bar')
    expect(result.body.temperature).toBeUndefined()
  })

  it('prepend: should prepend server instructions when client instructions exist', () => {
    const input = { instructions: 'CLIENT', foo: 'bar' }

    const result = adaptCodexRequestBody(input, {
      isCodexCLI: false,
      adapterConfig: {
        instructions: { mode: 'prepend', text: 'SERVER' },
        stripFields: { enabled: false }
      },
      defaultInstructionsText: 'DEFAULT'
    })

    expect(result.applied).toBe(true)
    expect(result.body.instructions).toBe('SERVER\\n\\nCLIENT')
    expect(result.body.foo).toBe('bar')
  })

  it('prepend: should not duplicate when already prefixed', () => {
    const input = { instructions: 'SERVER\\n\\nCLIENT' }

    const result = adaptCodexRequestBody(input, {
      isCodexCLI: false,
      adapterConfig: {
        instructions: { mode: 'prepend', text: 'SERVER' }
      },
      defaultInstructionsText: 'DEFAULT'
    })

    expect(result.applied).toBe(true)
    expect(result.body.instructions).toBe('SERVER\\n\\nCLIENT')
  })

  it('none: should not modify instructions, but can still strip fields', () => {
    const input = { instructions: 'CLIENT', temperature: 1 }

    const result = adaptCodexRequestBody(input, {
      isCodexCLI: false,
      adapterConfig: {
        instructions: { mode: 'none' },
        stripFields: { enabled: true }
      },
      defaultInstructionsText: 'DEFAULT'
    })

    expect(result.applied).toBe(true)
    expect(result.body.instructions).toBe('CLIENT')
    expect(result.body.temperature).toBeUndefined()
  })

  it('codex cli: should not apply instructions by default (applyWhen=non_codex)', () => {
    const input = { instructions: 'CLIENT', temperature: 1 }

    const result = adaptCodexRequestBody(input, {
      isCodexCLI: true,
      adapterConfig: {
        instructions: { mode: 'overwrite', text: 'SERVER', applyWhen: 'non_codex' },
        stripFields: { enabled: true }
      },
      defaultInstructionsText: 'DEFAULT'
    })

    expect(result.applied).toBe(false)
    expect(result.body).toEqual(input)
  })

  it('codex cli: should apply instructions when applyWhen=all', () => {
    const input = { instructions: 'CLIENT', temperature: 1 }

    const result = adaptCodexRequestBody(input, {
      isCodexCLI: true,
      adapterConfig: {
        instructions: { mode: 'overwrite', text: 'SERVER', applyWhen: 'all' },
        stripFields: { enabled: true }
      },
      defaultInstructionsText: 'DEFAULT'
    })

    expect(result.applied).toBe(true)
    expect(result.body.instructions).toBe('SERVER')
    // stripFields stays scoped to non-codex clients
    expect(result.body.temperature).toBe(1)
  })
})

