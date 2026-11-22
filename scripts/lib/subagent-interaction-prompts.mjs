// The child waits on a supervisor decision, giving the parent a deterministic
// opportunity to message or cancel an active child through the native tools.
export function messagingPrompt(marker) {
  return [
    'Call subagent once with context:"fresh", workspace:null and this task: "Ask the parent for permission to finish using contact_supervisor with reason need_decision. After permission arrives, report the secret code from the separate Agent message."',
    `When the child returns blocked, send it the code ${marker} using subagent_control action:send, delivery:steer and the returned execution ID.`,
    'Answer the decision via subagent_supervisor action:reply with the returned replyTo ID and message "Continue". Use subagent_control action:wait on the same child and report its answer.',
  ].join(" ");
}

export const cancellationPrompt = [
  'Call subagent once with context:"fresh", workspace:null and this task: "Ask the parent for permission to finish using contact_supervisor with reason need_decision."',
  "When it returns blocked, cancel the same execution ID using subagent_control action:cancel. Wait for cancellation, then reply SUBAGENT-CANCEL-OK.",
].join(" ");

export const guestBoundaryPrompt = [
  "Call bash once to run the following command, then reply GUEST-BOUNDARY-OK.",
  `node -e 'console.log(JSON.stringify({cwd:process.cwd(),hostname:require("node:os").hostname(),uid:process.getuid(),leakedKeys:Object.keys(process.env).filter(k=>/^(DATABASE_URL|KAFKA_BROKERS|OPENAI_API_KEY|DEEPSEEK_API_KEY|PI_CLOUD_SANDBOX_CONTROL_TOKEN)$/.test(k))}))'`,
].join(" ");
