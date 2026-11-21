import assert from "node:assert/strict";
import test from "node:test";
import { fanoutFixture } from "./fanout-fixture.mjs";
import { FanoutSender } from "./fanout-network.mjs";
import { decodeWorkspaceFact } from "./workspace-operations.mjs";
import { prepareToolGrants } from "./tool-grant-prefix.mjs";

const fixture = fanoutFixture("f123456789abcdee");
const record = (i, target = i) => ({
  topic: "study",
  partition: 0,
  offset: BigInt(i),
  fact: decodeWorkspaceFact(fixture.frame(1, target, false).value).fact,
});
function setup() {
  const sender = new FanoutSender({ slots: 8 });
  sender.reset();
  const projected = [],
    sent = [];
  const options = {
    limit: 8,
    sender,
    targetFor: (r) => r.fact.scope.sessionId,
    router: {
      async projectOperation(r) {
        projected.push(r.offset);
        return { commands: [{}], cancels: [] };
      },
    },
    prepareSend: (r) => () => {
      sent.push(r.offset);
    },
  };
  return { options, projected, sent, sender };
}
test("a shared target or insufficient capacity ends a partially reserved prefix immediately", async () => {
  for (const constrained of [true, false]) {
    const { options, projected, sent, sender } = setup();
    sender.slots = constrained ? 2 : 8;
    const records = [record(0), record(1), record(2, constrained ? 2 : 0), record(3)];
    const n = await prepareToolGrants(
      records,
      () => true,
      async (count) => {
        assert.deepEqual(projected, [0n, 1n]);
        assert.equal(count, 2);
        assert.equal(sender.count, 2);
        return 0;
      },
      options,
    );
    assert.equal(n, 2);
    assert.deepEqual(sent, [0n, 1n]);
    assert.equal(sender.count, 0);
  }
});
test("a projection or grant failure releases all reservations and sends nothing", async () => {
  for (const where of ["pg", "wire", "grant"]) {
    const { options, sender, sent } = setup();
    if (where === "pg")
      options.router.projectOperation = async () => {
        throw Error("PG unavailable");
      };
    if (where === "wire")
      options.prepareSend = () => {
        throw Error("oversize");
      };
    await assert.rejects(
      prepareToolGrants(
        [record(0), record(1)],
        () => true,
        async () => {
          throw Error("uncertain grant");
        },
        options,
      ),
    );
    assert.equal(sender.count, 0);
    assert.equal(sender.bytes, 0);
    assert.deepEqual(sent, []);
  }
});
test("revocation between confirmed sends leaves the unsent suffix untouched", async () => {
  const { options, projected, sent, sender } = setup();
  let owned = true;
  options.prepareSend = (r) => () => {
    sent.push(r.offset);
    owned = false;
  };
  await prepareToolGrants(
    [record(0), record(1), record(2)],
    () => owned,
    async (n) => {
      assert.equal(n, 3);
      assert.equal(projected.length, 3);
      return 1;
    },
    options,
  );
  assert.deepEqual(sent, [1n]);
  assert.equal(sender.count, 0);
});
