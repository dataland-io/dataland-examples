---
to: <%= h.changeCase.param(name) %>/spec.yaml
---

moduleId: <%= h.changeCase.param(name) %>-example

buildCommand: npm run build

workers:
  - workerId: <%= h.changeCase.param(name) %>
    scriptPath: dist/<%= h.changeCase.camel(name) %>.bundle.js
    triggers:
      transaction:
        enabled: true
