import { HeartbeatMonitor } from 'checkly/constructs'
import { slumberMill } from './group'

new HeartbeatMonitor('webhook-delivered', {
  name: 'webhook-delivered',
  description: "Pinged by every webhook to the `checkly-probe` tenant, whose webhook URL is this monitor's ping URL.",
  group: slumberMill,
  period: 15,
  periodUnit: 'minutes',
  grace: 15,
  graceUnit: 'minutes',
})
