import * as path from 'node:path'
import { Frequency, MultiStepCheck } from 'checkly/constructs'
import { slumberMill } from './group'

new MultiStepCheck('ingest-round-trip', {
  name: 'ingest-round-trip',
  group: slumberMill,
  frequency: Frequency.EVERY_15M,
  code: {
    entrypoint: path.join(__dirname, 'ingest-round-trip.spec.ts'),
  },
})
