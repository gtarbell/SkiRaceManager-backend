import { APIGatewayProxyEventV2, APIGatewayProxyResultV2 } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, QueryCommand, PutCommand, DeleteCommand, UpdateCommand, GetCommand } from "@aws-sdk/lib-dynamodb";

const ROSTERS = process.env.ROSTERS_TABLE!;
const RACERS = process.env.RACERS_TABLE!;
const TEAMS = process.env.TEAMS_TABLE!;
const RACES = process.env.RACES_TABLE!;

const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));

type RacerClass = "Varsity" | "Varsity Alternate" | "Jr Varsity" | "Provisional" | "DNS";
type InputRacerClass = RacerClass | "DNS - Did Not Start";
type Gender = "Male" | "Female";

const CAP = { Varsity: 5, "Varsity Alternate": 1 };
const CLASS_ORDER: RacerClass[] = ["Varsity", "Varsity Alternate", "Jr Varsity", "Provisional", "DNS"];

function k(raceId: string, teamId: string) { return `ROSTER#${raceId}#${teamId}`; }

async function getRoster(raceId: string, teamId: string) {
  const res = await ddb.send(new QueryCommand({
    TableName: ROSTERS,
    KeyConditionExpression: "pk = :pk",
    ExpressionAttributeValues: { ":pk": k(raceId, teamId) },
  }));
  // items have sk "<gender>#<class>#<raceId>#<racerId>"
  return (res.Items ?? []).map(i => ({
    raceId, teamId,
    racerId: i.racerId as string,
    gender: i.gender as Gender,
    class: i.class as RacerClass,
    startOrder: (i.startOrder as number | null) ?? null,
  }));
}

async function countRosterEntries(raceId: string, teamId: string) {
  const res = await ddb.send(new QueryCommand({
    TableName: ROSTERS,
    KeyConditionExpression: "pk = :pk",
    ExpressionAttributeValues: { ":pk": k(raceId, teamId) },
    Select: "COUNT",
  }));
  return res.Count ?? 0;
}

async function countInClass(raceId: string, teamId: string, gender: Gender, cls: RacerClass) {
  const res = await ddb.send(new QueryCommand({
    TableName: ROSTERS,
    KeyConditionExpression: "pk = :pk",
    FilterExpression: "#g = :g AND #c = :c",
    ExpressionAttributeNames: { "#g": "gender", "#c": "class" },
    ExpressionAttributeValues: { ":pk": k(raceId, teamId), ":g": gender, ":c": cls },
  }));
  return (res.Items ?? []).length;
}

async function isRaceLocked(raceId: string) {
  const res = await ddb.send(new GetCommand({ TableName: RACES, Key: { raceId } }));
  return Boolean(res.Item?.locked);
}

export const rosterRouter = async (e: APIGatewayProxyEventV2): Promise<APIGatewayProxyResultV2> => {
  const path = e.rawPath;
  const method = e.requestContext.http.method;
  if (method === "POST" && path.endsWith("/races/roster-counts")) {
    const body = JSON.parse(e.body || "{}") as { raceIds?: string[]; teamIds?: string[] };
    const raceIds = (body.raceIds ?? []).filter(Boolean);
    const teamIds = (body.teamIds ?? []).filter(Boolean);
    if (!raceIds.length || !teamIds.length) {
      return { statusCode: 400, body: JSON.stringify({ error: "raceIds and teamIds are required" }) };
    }
    const counts: Record<string, Record<string, number>> = {};
    for (const raceId of raceIds) {
      const perTeam: Record<string, number> = {};
      for (const teamId of teamIds) {
        perTeam[teamId] = await countRosterEntries(raceId, teamId);
      }
      counts[raceId] = perTeam;
    }
    return { statusCode: 200, body: JSON.stringify({ counts }) };
  }
  const params = e.pathParameters || {};
  const pathParts = path.split("/").filter(Boolean);
  const raceId = params["raceId"] ?? pathParts[pathParts.indexOf("races")+1];
  const teamId = params["teamId"] ?? pathParts[pathParts.indexOf("roster")+1];
  if (!raceId || !teamId) return { statusCode: 400, body: JSON.stringify({ error: "Missing raceId or teamId" }) };

  if (method !== "GET") {
    const locked = await isRaceLocked(raceId);
    if (locked) {
      return { statusCode: 400, body: JSON.stringify({ error: "Roster is locked for this race." }) };
    }
  }

  if (method === "GET") {
    const items = await getRoster(raceId, teamId);
    return { statusCode: 200, body: JSON.stringify(items) };
  }

  if (method === "POST" && path.endsWith("/copy")) {
    const { fromRaceId } = JSON.parse(e.body || "{}") as { fromRaceId?: string };
    if (!fromRaceId) return { statusCode: 400, body: JSON.stringify({ error: "fromRaceId required" }) };

    const source = await getRoster(fromRaceId, teamId);
    const existingTarget = await getRoster(raceId, teamId);

    for (const entry of existingTarget) {
      const keyDel = { pk: k(raceId, teamId), sk: `${entry.gender}#${entry.class}#${raceId}#${entry.racerId}` };
      await ddb.send(new DeleteCommand({ TableName: ROSTERS, Key: keyDel }));
    }

    const result: typeof source = [];
    const ordered = source
      .slice()
      .sort((a, b) => {
        if (a.gender !== b.gender) return a.gender.localeCompare(b.gender);
        const pa = CLASS_ORDER.indexOf(a.class);
        const pb = CLASS_ORDER.indexOf(b.class);
        const sa = a.startOrder ?? 0;
        const sb = b.startOrder ?? 0;
        return pa === pb ? sa - sb : pa - pb;
      });

    const countInResult = (gender: Gender, cls: RacerClass) =>
      result.filter(r => r.gender === gender && r.class === cls).length;
    const nextStartOrder = (gender: Gender, cls: RacerClass) => {
      if (cls === "DNS") return null;
      const max = result
        .filter(r => r.gender === gender && r.class === cls)
        .reduce((m, r) => Math.max(m, r.startOrder ?? 0), 0);
      return max + 1;
    };

    for (const entry of ordered) {
      if (entry.class === "DNS") continue; // skip copying DNS entries; they remain eligible but not on roster
      const cls = entry.class === "Provisional" ? "Provisional" : entry.class;
      if (cls === "Varsity" && countInResult(entry.gender, "Varsity") >= CAP.Varsity) continue;
      if (cls === "Varsity Alternate" && countInResult(entry.gender, "Varsity Alternate") >= CAP["Varsity Alternate"]) continue;

      const startOrder = nextStartOrder(entry.gender, cls);
      const item = {
        pk: k(raceId, teamId),
        sk: `${entry.gender}#${cls}#${raceId}#${entry.racerId}`,
        racerId: entry.racerId,
        gender: entry.gender,
        class: cls,
        startOrder,
      };
      await ddb.send(new PutCommand({ TableName: ROSTERS, Item: item }));
      result.push({ raceId, teamId, racerId: entry.racerId, gender: entry.gender, class: cls, startOrder: startOrder ?? null });
    }

    return { statusCode: 200, body: JSON.stringify(result) };
  }

  if (method === "POST" && path.endsWith("/add")) {
    const { racerId, desiredClass } = JSON.parse(e.body || "{}") as { racerId: string; desiredClass?: InputRacerClass };
    // Load racer baseline to enforce Provisional lock
    // (in a real app you'd also enforce coach scope here via auth context)
    const roster = await getRoster(raceId, teamId);

    // Pull racer (min fields) — for brevity assume request includes gender/class in UI; otherwise query table
    // You can store racer gender/class redundantly in roster items for speed.
    // Here we expect UI to send rGender/rClass if you want to skip extra lookups.

    // Find gender/class by reading from existing roster or requiring UI payload:
    const { rGender, rBaseClass } = JSON.parse(e.body || "{}") as { rGender: Gender; rBaseClass: RacerClass };

    const normalizedDesired = desiredClass === "DNS - Did Not Start" ? "DNS" : desiredClass;

    const cls: RacerClass = rBaseClass === "Provisional"
      ? (normalizedDesired === "DNS" ? "DNS" : "Provisional")
      : (normalizedDesired ?? rBaseClass);

    // enforce caps
    if (cls === "Varsity" && (await countInClass(raceId, teamId, rGender, "Varsity")) >= CAP.Varsity)
      return { statusCode: 400, body: JSON.stringify({ error: `Varsity is capped at 5 for ${rGender}.` }) };
    if (cls === "Varsity Alternate" && (await countInClass(raceId, teamId, rGender, "Varsity Alternate")) >= CAP["Varsity Alternate"])
      return { statusCode: 400, body: JSON.stringify({ error: `Varsity Alternate is capped at 1 for ${rGender}.` } )};

    // compute next startOrder (max+1 within gender+class)
    const bucket = roster.filter(e => e.gender === rGender && e.class === cls);
    const startOrder = cls === "DNS" ? null : (bucket.length ? Math.max(...bucket.map(b => b.startOrder ?? 0)) : 0) + 1;

    await ddb.send(new PutCommand({
      TableName: ROSTERS,
      Item: {
        pk: k(raceId, teamId),
        sk: `${rGender}#${cls}#${raceId}#${racerId}`,
        racerId,
        gender: rGender,
        class: cls,
        startOrder,
      },
      ConditionExpression: "attribute_not_exists(pk) OR attribute_not_exists(sk)",
    }));

    const items = await getRoster(raceId, teamId);
    return { statusCode: 200, body: JSON.stringify(items )};
  }

  if (method === "PATCH") {
    const body = JSON.parse(e.body || "{}");
    const racerId = e.pathParameters?.["racerId"]!;
    const { newClass } = body as { newClass: InputRacerClass };
    const normalizedNewClass: RacerClass = newClass === "DNS - Did Not Start" ? "DNS" : newClass;
    const roster = await getRoster(raceId, teamId);
    const entry = roster.find(r => r.racerId === racerId);
    if (!entry) return { statusCode: 404, body:JSON.stringify( { error: "Entry not found" } )};

    if (entry.class !== normalizedNewClass) {
      // enforce provisional lock + caps
      if (entry.class === "Provisional" && normalizedNewClass !== "Provisional" && normalizedNewClass !== "DNS")
        return { statusCode: 400, body: JSON.stringify({ error: "Provisional racers must remain Provisional for all races." }) };

      if (normalizedNewClass === "Varsity" && (await countInClass(raceId, teamId, entry.gender, "Varsity")) >= CAP.Varsity)
        return { statusCode: 400, body: JSON.stringify({ error: `Varsity is capped at 5 for ${entry.gender}.` } )};
      if (normalizedNewClass === "Varsity Alternate" && (await countInClass(raceId, teamId, entry.gender, "Varsity Alternate")) >= CAP["Varsity Alternate"])
        return { statusCode: 400, body: JSON.stringify({ error: `Varsity Alternate is capped at 1 for ${entry.gender}.` } )};

      const oldBucket = entry.class === "DNS" ? [] : roster
        .filter(r => r.gender === entry.gender && r.class === entry.class)
        .sort((a, b) => (a.startOrder ?? 0) - (b.startOrder ?? 0));
      // delete old + insert new with new start order
      await ddb.send(new DeleteCommand({
        TableName: ROSTERS,
        Key: { pk: k(raceId, teamId), sk: `${entry.gender}#${entry.class}#${raceId}#${racerId}` },
      }));
      // Shift startOrder for racers after the moved entry within the old bucket
      if (entry.class !== "DNS") {
        const toShift = oldBucket.filter(r => (r.startOrder ?? 0) > (entry.startOrder ?? 0));
        for (const racer of toShift) {
          const oldKey = { pk: k(raceId, teamId), sk: `${racer.gender}#${racer.class}#${raceId}#${racer.racerId}` };
          const newStartOrder = (racer.startOrder ?? 0) - 1;
          const newKey = { pk: k(raceId, teamId), sk: `${racer.gender}#${racer.class}#${raceId}#${racer.racerId}` };
          await ddb.send(new DeleteCommand({ TableName: ROSTERS, Key: oldKey }));
          await ddb.send(new PutCommand({
            TableName: ROSTERS,
            Item: {
              ...newKey,
              racerId: racer.racerId,
              gender: racer.gender,
              class: racer.class,
              startOrder: newStartOrder,
            }
          }));
        }
      }
      const bucket = roster.filter(e => e.gender === entry.gender && e.class === normalizedNewClass);
      const startOrder = normalizedNewClass === "DNS" ? null : (bucket.length ? Math.max(...bucket.map(b => b.startOrder ?? 0)) : 0) + 1;
      await ddb.send(new PutCommand({
        TableName: ROSTERS,
        Item: {
          pk: k(raceId, teamId),
          sk: `${entry.gender}#${normalizedNewClass}#${raceId}#${racerId}`,
          racerId,
          gender: entry.gender,
          class: normalizedNewClass,
          startOrder,
        }
      }));
    }
    const items = await getRoster(raceId, teamId);
    return { statusCode: 200, body: JSON.stringify(items) };
  }

  if (method === "DELETE") {
    const racerId = e.pathParameters?.["racerId"]!;
    const roster = await getRoster(raceId, teamId);
    const entry = roster.find(r => r.racerId === racerId);
    if (!entry) return { statusCode: 200, body: JSON.stringify(await getRoster(raceId, teamId)) };
    const bucket = entry.class === "DNS" ? [] : roster
      .filter(r => r.gender === entry.gender && r.class === entry.class)
      .sort((a, b) => (a.startOrder ?? 0) - (b.startOrder ?? 0));
    await ddb.send(new DeleteCommand({
      TableName: ROSTERS,
      Key: { pk: k(raceId, teamId), sk: `${entry.gender}#${entry.class}#${raceId}#${racerId}` },
    }));
    if (entry.class !== "DNS") {
      const toShift = bucket.filter(r => (r.startOrder ?? 0) > (entry.startOrder ?? 0));
      for (const racer of toShift) {
        const oldKey = { pk: k(raceId, teamId), sk: `${racer.gender}#${racer.class}#${raceId}#${racer.racerId}` };
        const newStartOrder = (racer.startOrder ?? 0) - 1;
        const newKey = { pk: k(raceId, teamId), sk: `${racer.gender}#${racer.class}#${raceId}#${racer.racerId}` };
        await ddb.send(new DeleteCommand({ TableName: ROSTERS, Key: oldKey }));
        await ddb.send(new PutCommand({
          TableName: ROSTERS,
          Item: {
            ...newKey,
            racerId: racer.racerId,
            gender: racer.gender,
            class: racer.class,
            startOrder: newStartOrder,
          }
        }));
      }
    }
    return { statusCode: 200, body: JSON.stringify(await getRoster(raceId, teamId) )};
  }

  if (method === "POST" && path.endsWith("/move")) {
    const { racerId, direction } = JSON.parse(e.body || "{}") as { racerId: string; direction: "up" | "down" };
    const roster = await getRoster(raceId, teamId);
    const entry = roster.find(r => r.racerId === racerId);
    if (!entry) return { statusCode: 404, body: JSON.stringify({ error: "Entry not found" } )};
    if (entry.class === "DNS")
      return { statusCode: 400, body: JSON.stringify({ error: "DNS racers are not in the start order." }) };

    const entryKey = { pk: k(raceId, teamId), sk: `${entry.gender}#${entry.class}#${raceId}#${entry.racerId}` };

    // Special behavior for Varsity Alternate:
    // - Up: move to last Varsity spot; if Varsity already has position 5, swap that racer into Varsity Alternate.
    // - Down: swap with #1 Jr Varsity.
    if (entry.class === "Varsity Alternate") {
      const varsity = roster
        .filter(r => r.gender === entry.gender && r.class === "Varsity" && r.startOrder != null)
        .sort((a, b) => (a.startOrder ?? 0) - (b.startOrder ?? 0));
      const jrVarsity = roster
        .filter(r => r.gender === entry.gender && r.class === "Jr Varsity" && r.startOrder != null)
        .sort((a, b) => (a.startOrder ?? 0) - (b.startOrder ?? 0));

      if (direction === "up") {
        const varsityCap = CAP.Varsity;
        const varsityFifth = varsity.find(v => (v.startOrder ?? 0) === varsityCap);

        // remove current Varsity Alternate entry
        await ddb.send(new DeleteCommand({ TableName: ROSTERS, Key: entryKey }));

        if (varsityFifth) {
          const fifthKey = { pk: k(raceId, teamId), sk: `${varsityFifth.gender}#${varsityFifth.class}#${raceId}#${varsityFifth.racerId}` };
          await ddb.send(new DeleteCommand({ TableName: ROSTERS, Key: fifthKey }));

          await ddb.send(new PutCommand({
            TableName: ROSTERS,
            Item: {
              ...entryKey,
              sk: `${entry.gender}#Varsity#${raceId}#${entry.racerId}`,
              racerId: entry.racerId,
              gender: entry.gender,
              class: "Varsity",
              startOrder: varsityCap,
            }
          }));
          await ddb.send(new PutCommand({
            TableName: ROSTERS,
            Item: {
              ...fifthKey,
              sk: `${varsityFifth.gender}#Varsity Alternate#${raceId}#${varsityFifth.racerId}`,
              racerId: varsityFifth.racerId,
              gender: varsityFifth.gender,
              class: "Varsity Alternate",
              startOrder: entry.startOrder ?? 1,
            }
          }));
        } else {
          const nextOrder = (varsity.length ? Math.max(...varsity.map(v => v.startOrder ?? 0)) : 0) + 1;
          await ddb.send(new PutCommand({
            TableName: ROSTERS,
            Item: {
              ...entryKey,
              sk: `${entry.gender}#Varsity#${raceId}#${entry.racerId}`,
              racerId: entry.racerId,
              gender: entry.gender,
              class: "Varsity",
              startOrder: nextOrder,
            }
          }));
        }

        return { statusCode: 200, body: JSON.stringify(await getRoster(raceId, teamId)) };
      }

      if (direction === "down") {
        const topJv = jrVarsity.find(j => (j.startOrder ?? 0) === 1);
        if (!topJv) {
          // No JV exists; move VA into first JV slot
          await ddb.send(new DeleteCommand({ TableName: ROSTERS, Key: entryKey }));
          await ddb.send(new PutCommand({
            TableName: ROSTERS,
            Item: {
              ...entryKey,
              sk: `${entry.gender}#Jr Varsity#${raceId}#${entry.racerId}`,
              racerId: entry.racerId,
              gender: entry.gender,
              class: "Jr Varsity",
              startOrder: 1,
            }
          }));
        } else {
          const topJvKey = { pk: k(raceId, teamId), sk: `${topJv.gender}#${topJv.class}#${raceId}#${topJv.racerId}` };

          await ddb.send(new DeleteCommand({ TableName: ROSTERS, Key: entryKey }));
          await ddb.send(new DeleteCommand({ TableName: ROSTERS, Key: topJvKey }));

          await ddb.send(new PutCommand({
            TableName: ROSTERS,
            Item: {
              ...entryKey,
              sk: `${entry.gender}#Jr Varsity#${raceId}#${entry.racerId}`,
              racerId: entry.racerId,
              gender: entry.gender,
              class: "Jr Varsity",
              startOrder: 1,
            }
          }));
          await ddb.send(new PutCommand({
            TableName: ROSTERS,
            Item: {
              ...topJvKey,
              sk: `${topJv.gender}#Varsity Alternate#${raceId}#${topJv.racerId}`,
              racerId: topJv.racerId,
              gender: topJv.gender,
              class: "Varsity Alternate",
              startOrder: entry.startOrder ?? 1,
            }
          }));
        }

        return { statusCode: 200, body: JSON.stringify(await getRoster(raceId, teamId)) };
      }
    }

    // Special behavior for #1 Jr Varsity moving up: promote to Varsity Alternate (swapping if needed)
    if (entry.class === "Jr Varsity" && direction === "up" && (entry.startOrder ?? 0) === 1) {
      const va = roster.find(r => r.gender === entry.gender && r.class === "Varsity Alternate");
      const jvBucket = roster
        .filter(r => r.gender === entry.gender && r.class === "Jr Varsity" && r.startOrder != null)
        .sort((a, b) => (a.startOrder ?? 0) - (b.startOrder ?? 0));

      // Remove JV entry
      await ddb.send(new DeleteCommand({ TableName: ROSTERS, Key: entryKey }));

      // If no existing VA, shift remaining JV start orders down to fill the gap
      if (!va) {
        const toShift = jvBucket.filter(r => (r.startOrder ?? 0) > (entry.startOrder ?? 0));
        for (const racer of toShift) {
          const oldKey = { pk: k(raceId, teamId), sk: `${racer.gender}#${racer.class}#${raceId}#${racer.racerId}` };
          const newStartOrder = (racer.startOrder ?? 0) - 1;
          await ddb.send(new DeleteCommand({ TableName: ROSTERS, Key: oldKey }));
          await ddb.send(new PutCommand({
            TableName: ROSTERS,
            Item: {
              ...oldKey,
              racerId: racer.racerId,
              gender: racer.gender,
              class: racer.class,
              startOrder: newStartOrder,
            }
          }));
        }

        await ddb.send(new PutCommand({
          TableName: ROSTERS,
          Item: {
            ...entryKey,
            sk: `${entry.gender}#Varsity Alternate#${raceId}#${entry.racerId}`,
            racerId: entry.racerId,
            gender: entry.gender,
            class: "Varsity Alternate",
            startOrder: 1,
          }
        }));
      } else {
        // Swap JV #1 with existing VA
        const vaKey = { pk: k(raceId, teamId), sk: `${va.gender}#${va.class}#${raceId}#${va.racerId}` };
        await ddb.send(new DeleteCommand({ TableName: ROSTERS, Key: vaKey }));

        await ddb.send(new PutCommand({
          TableName: ROSTERS,
          Item: {
            ...entryKey,
            sk: `${entry.gender}#Varsity Alternate#${raceId}#${entry.racerId}`,
            racerId: entry.racerId,
            gender: entry.gender,
            class: "Varsity Alternate",
            startOrder: va.startOrder ?? 1,
          }
        }));
        await ddb.send(new PutCommand({
          TableName: ROSTERS,
          Item: {
            ...vaKey,
            sk: `${va.gender}#Jr Varsity#${raceId}#${va.racerId}`,
            racerId: va.racerId,
            gender: va.gender,
            class: "Jr Varsity",
            startOrder: entry.startOrder ?? 1,
          }
        }));
      }

      return { statusCode: 200, body: JSON.stringify(await getRoster(raceId, teamId)) };
    }

    // Special behavior: last Varsity moving down swaps/moves into Varsity Alternate
    if (entry.class === "Varsity" && direction === "down") {
      const varsityBucket = roster
        .filter(r => r.gender === entry.gender && r.class === "Varsity" && r.startOrder != null)
        .sort((a, b) => (a.startOrder ?? 0) - (b.startOrder ?? 0));
      if (varsityBucket.some(r => r.startOrder == null)) {
        return { statusCode: 500, body: JSON.stringify({ error: "Start order missing for roster entries." }) };
      }
      const lastVarsity = varsityBucket[varsityBucket.length - 1];
      const isLast = lastVarsity && (entry.startOrder ?? 0) === (lastVarsity.startOrder ?? 0);
      if (isLast) {
        const va = roster.find(r => r.gender === entry.gender && r.class === "Varsity Alternate");
        await ddb.send(new DeleteCommand({ TableName: ROSTERS, Key: entryKey }));

        if (va) {
          const vaKey = { pk: k(raceId, teamId), sk: `${va.gender}#${va.class}#${raceId}#${va.racerId}` };
          await ddb.send(new DeleteCommand({ TableName: ROSTERS, Key: vaKey }));

          await ddb.send(new PutCommand({
            TableName: ROSTERS,
            Item: {
              ...entryKey,
              sk: `${entry.gender}#Varsity Alternate#${raceId}#${entry.racerId}`,
              racerId: entry.racerId,
              gender: entry.gender,
              class: "Varsity Alternate",
              startOrder: va.startOrder ?? 1,
            }
          }));
          await ddb.send(new PutCommand({
            TableName: ROSTERS,
            Item: {
              ...vaKey,
              sk: `${va.gender}#Varsity#${raceId}#${va.racerId}`,
              racerId: va.racerId,
              gender: va.gender,
              class: "Varsity",
              startOrder: entry.startOrder ?? (varsityBucket.length || 1),
            }
          }));
        } else {
          await ddb.send(new PutCommand({
            TableName: ROSTERS,
            Item: {
              ...entryKey,
              sk: `${entry.gender}#Varsity Alternate#${raceId}#${entry.racerId}`,
              racerId: entry.racerId,
              gender: entry.gender,
              class: "Varsity Alternate",
              startOrder: 1,
            }
          }));
        }

        return { statusCode: 200, body: JSON.stringify(await getRoster(raceId, teamId)) };
      }
    }

    // swap startOrder within bucket
    const bucket = roster.filter(r => r.gender === entry.gender && r.class === entry.class);
    if (bucket.some(r => r.startOrder == null)) {
      return { statusCode: 500, body: JSON.stringify({ error: "Start order missing for roster entries." }) };
    }
    bucket.sort((a, b) => (a.startOrder ?? 0) - (b.startOrder ?? 0));
    const i = bucket.findIndex(b => b.racerId === racerId);
    if ((direction === "up" && i === 0) || (direction === "down" && i === bucket.length-1))
      return { statusCode: 200, body: JSON.stringify(roster) };

    const swapWith = bucket[direction === "up" ? i - 1 : i + 1];
    // swap by rewriting items (delete+put)
    const keyA = entryKey;
    const keyB = { pk: k(raceId, teamId), sk: `${swapWith.gender}#${swapWith.class}#${raceId}#${swapWith.racerId}` };

    await ddb.send(new DeleteCommand({ TableName: ROSTERS, Key: keyA }));
    
    await ddb.send(new DeleteCommand({ TableName: ROSTERS, Key: keyB }));
    

    await ddb.send(new PutCommand({
      TableName: ROSTERS, Item: {
        ...keyA, racerId: entry.racerId, gender: entry.gender, class: entry.class, startOrder: swapWith.startOrder!
      }
    }));
    await ddb.send(new PutCommand({
      TableName: ROSTERS, Item: {
        ...keyB, racerId: swapWith.racerId, gender: swapWith.gender, class: swapWith.class, startOrder: entry.startOrder!
      }
    }));

    return { statusCode: 200, body: JSON.stringify(await getRoster(raceId, teamId) )};
    //return { statusCode: 200, body: JSON.stringify( {"keyA": keyA, "KeyB": keyB} )};
  }

  return { statusCode: 404, body: JSON.stringify({ error: "Not found" }) };
};
