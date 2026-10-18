import { ApiCheck, AssertionBuilder, Frequency } from 'checkly/constructs'
import { slumberMill } from './group'

new ApiCheck('api-readyz', {
  name: 'api-readyz',
  group: slumberMill,
  frequency: Frequency.EVERY_5M,
  request: {
    method: 'GET',
    url: '{{SLUMBER_MILL_BASE_URL}}/readyz',
    assertions: [AssertionBuilder.statusCode().equals(200)],
  },
})
