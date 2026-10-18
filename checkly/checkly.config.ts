import { defineConfig } from 'checkly'

export default defineConfig({
  projectName: 'SlumberMill',
  logicalId: 'slumber-mill',
  repoUrl: 'https://github.com/zanvd/slumber-mill',
  checks: {
    checkMatch: 'checks/**/*.check.ts',
    ignoreDirectoriesMatch: ['node_modules/**'],
  },
})
