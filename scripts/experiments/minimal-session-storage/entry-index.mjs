// Recover the exact candidate DDL when temporarily switching comparison arms.
import { readFileSync } from "node:fs";
const schema = readFileSync(new URL("./schema.sql", import.meta.url), "utf8");
export const studyEntryIndexSql = schema
  .split("\n")
  .find((line) => line.startsWith("create index messages_conversation_entry "));
if (!studyEntryIndexSql) throw Error("Missing candidate Conversation Entry index");
