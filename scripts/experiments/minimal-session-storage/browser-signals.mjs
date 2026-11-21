import { readFile, writeFile, rename } from "node:fs/promises";
import { join } from "node:path";
import { until } from "./family-process.mjs";
export async function browserNote(dir, name, data = {}) {
  const dest = join(dir, "web-" + name + ".json");
  await writeFile(dest + ".part", JSON.stringify(data), { mode: 0o600 });
  await rename(dest + ".part", dest);
}
export async function browserMessage(dir, name) {
  return until(
    async () => {
      for (const n of ["failure", name]) {
        try {
          const v = JSON.parse(await readFile(join(dir, "web-" + n + ".json"), "utf8"));
          if (n === "failure") throw Error(v.message);
          return v;
        } catch (e) {
          if (e.code !== "ENOENT") throw e;
        }
      }
    },
    "browser " + name,
    90000,
  );
}
