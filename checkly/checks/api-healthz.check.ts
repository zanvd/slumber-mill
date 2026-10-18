import { ApiCheck, AssertionBuilder, Frequency } from 'checkly/constructs'
import { slumberMill } from './group'

new ApiCheck('api-healthz', {
  name: 'api-healthz',
  group: slumberMill,
  frequency: Frequency.EVERY_5M,
  request: {
    method: 'GET',
    url: '{{SLUMBER_MILL_BASE_URL}}/healthz',
    assertions: [AssertionBuilder.statusCode().equals(200)],
  },
})
