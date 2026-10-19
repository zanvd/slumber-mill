import { AlertChannel, CheckGroupV2 } from 'checkly/constructs'

export const slumberMill = new CheckGroupV2('slumber-mill', {
  name: 'slumber-mill',
  privateLocations: ['slumber-mill-local'],
  environmentVariables: [
    { key: 'SLUMBER_MILL_BASE_URL', value: 'http://api:8080' },
  ],
  alertChannels: [AlertChannel.fromId(377057)],
  // Without a policy, a CheckGroupV2 ignores its alert channels and each check keeps its own settings.
  alertEscalationPolicy: 'global',
})
