export async function serve(start) {
  const service = await start();
  let closing;
  const stop = () => {
    closing ??= service.close().catch((error) => {
      console.error("Session role shutdown failed", error.name);
      process.exitCode = 1;
    });
  };
  service.failed?.catch((error) => {
    process.exitCode = 1;
    console.error("Session role failed", error.name);
    stop();
  });
  process.once("SIGTERM", stop);
  process.once("SIGINT", stop);
}
