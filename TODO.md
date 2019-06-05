# TODO

- [ ] Monitoring: APM instrumentation
- [ ] Tracking: Change API to only commit tracking table data during
      loading phase so that a failure to write will not result in data
      not being retried. Use goque:
      https://github.com/beeker1121/goque
- [ ] Loader: Failures should be written to a holding location so that
      they can be retried as they have already been removed/adjusted
      from the original db's tracking table
- [ ] Migrator/DB: Should move configuration to support multiple table
      migrations per database/connection for better use of connection
      pools
- [ ] UI: Package as app with instrumentation exposure, etc

