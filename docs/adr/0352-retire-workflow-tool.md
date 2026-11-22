# ADR-0352. Retire sandbox-scripted delegation

The supported model catalogue delegates directly with `subagent` and manages
children with `subagent_control`. Remove the built-in `workflow` and its guest
callback route from the candidate. Bash and file tools keep their existing
sandbox boundaries. A guest script's stdout is tool output. Only an admitted
native tool request can create a child.

Claude Code offers a separate [dynamic workflow runtime](https://code.claude.com/docs/en/workflows#behavior-and-limits)
for programmatic fan-out. The script has no direct filesystem or shell access.
OpenAI's [managed multi-agent interface](https://developers.openai.com/api/docs/guides/agents-api/multi-agent)
offers creation, messaging, waiting and interruption in the harness. Both are
useful precedents. For this project the measured recovery cases require direct
delegation, and a sandbox script with live control callbacks adds another
failure boundary. Historical messages retain their original tool names. A fresh dispatch of
a retired workflow request returns a not-started error before allocation. On
replay the Projector skips script-originated control references. Ordinary
delegation keeps its original operation ID, child identity and recovery
reference. Guest stdout never triggers a second round of child creation.

This is a matched candidate change. Drain active writers under the existing
release procedure before replacing a running deployment.
