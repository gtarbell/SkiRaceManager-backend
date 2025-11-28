import { APIGatewayProxyEventV2, APIGatewayProxyResultV2 } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, QueryCommand, PutCommand, DeleteCommand, UpdateCommand } from "@aws-sdk/lib-dynamodb";

const ROSTERS = process.env.ROSTERS_TABLE!;
const RACERS = process.env.RACERS_TABLE!;
const TEAMS = process.env.TEAMS_TABLE!;

const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));

type RacerClass = "Varsity" | "Varsity Alternate" | "Jr Varsity" | "Provisional";
type Gender = "Male" | "Female";

const CAP = { Varsity: 5, "Varsity Alternate": 1 };

function k(raceId: string, teamId: string) { return `ROSTER#${raceId}#${teamId}`; }

async function getRoster(raceId: string, teamId: string) {
  const res = await ddb.send(new QueryCommand({
    TableName: ROSTERS,
    KeyConditionExpression: "pk = :pk",
    ExpressionAttributeValues: { ":pk": k(raceId, teamId) },
  }));
  // items have sk "<gender>#<class>#<startOrder>#<racerId>"
  return (res.Items ?? []).map(i => ({
    raceId, teamId,
    racerId: i.racerId as string,
    gender: i.gender as Gender,
    class: i.class as RacerClass,
    startOrder: i.startOrder as number,
  }));
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

export const rosterRouter = async (e: APIGatewayProxyEventV2): Promise<APIGatewayProxyResultV2> => {
  const path = e.rawPath;
  const method = e.requestContext.http.method;
  const params = e.pathParameters || {};
  const raceId = params["raceId"]!;
  const teamId = params["teamId"]!;

  if (method === "GET") {
    const items = await getRoster(raceId, teamId);
    return { statusCode: 200, body: JSON.stringify(items) };
  }

  if (method === "POST" && path.endsWith("/add")) {
    const { racerId, desiredClass } = JSON.parse(e.body || "{}") as { racerId: string; desiredClass?: RacerClass };
    // Load racer baseline to enforce Provisional lock
    // (in a real app you'd also enforce coach scope here via auth context)
    const roster = await getRoster(raceId, teamId);

    // Pull racer (min fields) — for brevity assume request includes gender/class in UI; otherwise query table
    // You can store racer gender/class redundantly in roster items for speed.
    // Here we expect UI to send rGender/rClass if you want to skip extra lookups.

    // Find gender/class by reading from existing roster or requiring UI payload:
    const { rGender, rBaseClass } = JSON.parse(e.body || "{}") as { rGender: Gender; rBaseClass: RacerClass };

    const cls: RacerClass = rBaseClass === "Provisional" ? "Provisional" : (desiredClass ?? rBaseClass);

    // enforce caps
    if (cls === "Varsity" && (await countInClass(raceId, teamId, rGender, "Varsity")) >= CAP.Varsity)
      return { statusCode: 400, body: JSON.stringify({ error: `Varsity is capped at 5 for ${rGender}.` }) };
    if (cls === "Varsity Alternate" && (await countInClass(raceId, teamId, rGender, "Varsity Alternate")) >= CAP["Varsity Alternate"])
      return { statusCode: 400, body: JSON.stringify({ error: `Varsity Alternate is capped at 1 for ${rGender}.` } )};

    // compute next startOrder (max+1 within gender+class)
    const bucket = roster.filter(e => e.gender === rGender && e.class === cls);
    const startOrder = (bucket.length ? Math.max(...bucket.map(b => b.startOrder)) : 0) + 1;

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
    const { newClass } = body as { newClass: RacerClass };
    const roster = await getRoster(raceId, teamId);
    const entry = roster.find(r => r.racerId === racerId);
    if (!entry) return { statusCode: 404, body:JSON.stringify( { error: "Entry not found" } )};

    if (entry.class !== newClass) {
      // enforce provisional lock + caps
      if (entry.class === "Provisional" && newClass !== "Provisional")
        return { statusCode: 400, body: JSON.stringify({ error: "Provisional racers must remain Provisional for all races." }) };

      if (newClass === "Varsity" && (await countInClass(raceId, teamId, entry.gender, "Varsity")) >= CAP.Varsity)
        return { statusCode: 400, body: JSON.stringify({ error: `Varsity is capped at 5 for ${entry.gender}.` } )};
      if (newClass === "Varsity Alternate" && (await countInClass(raceId, teamId, entry.gender, "Varsity Alternate")) >= CAP["Varsity Alternate"])
        return { statusCode: 400, body: JSON.stringify({ error: `Varsity Alternate is capped at 1 for ${entry.gender}.` } )};

      const oldBucket = roster
        .filter(r => r.gender === entry.gender && r.class === entry.class)
        .sort((a, b) => a.startOrder - b.startOrder);
      // delete old + insert new with new start order
      await ddb.send(new DeleteCommand({
        TableName: ROSTERS,
        Key: { pk: k(raceId, teamId), sk: `${entry.gender}#${entry.class}#${raceId}#${racerId}` },
      }));
      // Shift startOrder for racers after the moved entry within the old bucket
      const toShift = oldBucket.filter(r => r.startOrder > entry.startOrder);
      for (const racer of toShift) {
        const oldKey = { pk: k(raceId, teamId), sk: `${racer.gender}#${racer.class}#${raceId}#${racer.racerId}` };
        const newStartOrder = racer.startOrder - 1;
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
      const bucket = roster.filter(e => e.gender === entry.gender && e.class === newClass);
      const startOrder = (bucket.length ? Math.max(...bucket.map(b => b.startOrder)) : 0) + 1;
      await ddb.send(new PutCommand({
        TableName: ROSTERS,
        Item: {
          pk: k(raceId, teamId),
          sk: `${entry.gender}#${newClass}#${raceId}#${racerId}`,
          racerId,
          gender: entry.gender,
          class: newClass,
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
    const bucket = roster
      .filter(r => r.gender === entry.gender && r.class === entry.class)
      .sort((a, b) => a.startOrder - b.startOrder);
    await ddb.send(new DeleteCommand({
      TableName: ROSTERS,
      Key: { pk: k(raceId, teamId), sk: `${entry.gender}#${entry.class}#${raceId}#${racerId}` },
    }));
    const toShift = bucket.filter(r => r.startOrder > entry.startOrder);
    for (const racer of toShift) {
      const oldKey = { pk: k(raceId, teamId), sk: `${racer.gender}#${racer.class}#${raceId}#${racer.racerId}` };
      const newStartOrder = racer.startOrder - 1;
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
    return { statusCode: 200, body: JSON.stringify(await getRoster(raceId, teamId) )};
  }

  if (method === "POST" && path.endsWith("/move")) {
    const { racerId, direction } = JSON.parse(e.body || "{}") as { racerId: string; direction: "up" | "down" };
    const roster = await getRoster(raceId, teamId);
    const entry = roster.find(r => r.racerId === racerId);
    if (!entry) return { statusCode: 404, body: JSON.stringify({ error: "Entry not found" } )};
    // swap startOrder within bucket
    const bucket = roster.filter(r => r.gender === entry.gender && r.class === entry.class).sort((a,b)=>a.startOrder-b.startOrder);
    const i = bucket.findIndex(b => b.racerId === racerId);
    if ((direction === "up" && i === 0) || (direction === "down" && i === bucket.length-1))
      return { statusCode: 200, body: JSON.stringify(roster) };

    const swapWith = bucket[direction === "up" ? i - 1 : i + 1];
    // swap by rewriting items (delete+put)
    const keyA = { pk: k(raceId, teamId), sk: `${entry.gender}#${entry.class}#${raceId}#${entry.racerId}` };
    const keyB = { pk: k(raceId, teamId), sk: `${swapWith.gender}#${swapWith.class}#${raceId}#${swapWith.racerId}` };

    await ddb.send(new DeleteCommand({ TableName: ROSTERS, Key: keyA }));
    
    await ddb.send(new DeleteCommand({ TableName: ROSTERS, Key: keyB }));
    

    await ddb.send(new PutCommand({
      TableName: ROSTERS, Item: {
        ...keyA, racerId: entry.racerId, gender: entry.gender, class: entry.class, startOrder: swapWith.startOrder
      }
    }));
    await ddb.send(new PutCommand({
      TableName: ROSTERS, Item: {
        ...keyB, racerId: swapWith.racerId, gender: swapWith.gender, class: swapWith.class, startOrder: entry.startOrder
      }
    }));

    return { statusCode: 200, body: JSON.stringify(await getRoster(raceId, teamId) )};
    //return { statusCode: 200, body: JSON.stringify( {"keyA": keyA, "KeyB": keyB} )};
  }

  return { statusCode: 404, body: JSON.stringify({ error: "Not found" }) };
};
