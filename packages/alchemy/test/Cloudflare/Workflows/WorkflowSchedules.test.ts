import { makeWorkflowName } from "@/Cloudflare/Workflows/WorkflowName.ts";
import * as Cloudflare from "@/Cloudflare/index.ts";
import * as Test from "@/Test/Alchemy";
import * as workflows from "@distilled.cloud/cloudflare/workflows";
import { expect } from "alchemy-test";
import * as Effect from "effect/Effect";
import * as Schedule from "effect/Schedule";

const { test } = Test.make({
  providers: Cloudflare.providers(),
});

const workflowScript = `import { WorkflowEntrypoint } from "cloudflare:workers";
export class ScheduledWorkflow extends WorkflowEntrypoint {
  async run() {
    return "ok";
  }
}
export default {
  async fetch() {
    return new Response("ok");
  },
};
`;

const workflowProgram = (schedules: string[]) =>
  Effect.gen(function* () {
    const worker = yield* Cloudflare.Worker("workflow-schedules-worker", {
      script: workflowScript,
      env: {
        WORKFLOW: Cloudflare.Workflow("ScheduledWorkflow", {
          schedules,
        }),
      },
    });
    return {
      accountId: worker.accountId,
      workerName: worker.workerName,
      workflowName: makeWorkflowName(worker.workerName, "ScheduledWorkflow"),
    };
  });

const readSchedules = (accountId: string, workflowName: string) =>
  workflows
    .getWorkflow({ accountId, workflowName })
    .pipe(
      Effect.map((workflow) =>
        (workflow.schedules ?? []).map((schedule) => schedule.cron).sort(),
      ),
    );

test(
  "props-only Workflow reference carries schedules for the async binding",
  Effect.sync(() => {
    const ref = Cloudflare.Workflow("ScheduledWorkflow", {
      schedules: ["0 0 29 2 *"],
    });
    expect(ref.className).toBe("ScheduledWorkflow");
    expect(ref.schedules).toEqual(["0 0 29 2 *"]);
  }),
);

test.provider(
  "creates, updates, and removes Workflow schedules",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();

      const first = ["0 0 29 2 *"];
      const deployed = yield* stack.deploy(workflowProgram(first));
      expect(
        yield* readSchedules(deployed.accountId, deployed.workflowName),
      ).toEqual(first);

      const second = ["0 0 29 2 *", "0 1 29 2 *"];
      yield* stack.deploy(workflowProgram(second));
      expect(
        yield* readSchedules(deployed.accountId, deployed.workflowName),
      ).toEqual([...second].sort());

      yield* stack.deploy(workflowProgram([]));
      expect(
        yield* readSchedules(deployed.accountId, deployed.workflowName),
      ).toEqual([]);

      yield* stack.destroy();

      const gone = yield* workflows
        .getWorkflow({
          accountId: deployed.accountId,
          workflowName: deployed.workflowName,
        })
        .pipe(
          Effect.as(false),
          Effect.catchTag("WorkflowNotFound", () => Effect.succeed(true)),
          Effect.repeat({
            schedule: Schedule.spaced("1 second"),
            until: (deleted) => deleted,
            times: 10,
          }),
        );
      expect(gone).toBe(true);
    }),
  { timeout: 180_000 },
);
