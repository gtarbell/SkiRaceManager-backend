import { APIGatewayProxyEventV2, APIGatewayProxyResultV2 } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, ScanCommand, GetCommand, UpdateCommand } from "@aws-sdk/lib-dynamodb";

const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const RACES = process.env.RACES_TABLE!;
const RACE_TYPES = new Set(["Slalom", "Giant Slalom"]);

function normalizeRace<T extends Record<string, any>>(raw: T): T & { locked: boolean; independent: boolean } {
  return { ...raw, locked: Boolean(raw.locked), independent: Boolean((raw as any).independent) };
}

function getRaceId(e: APIGatewayProxyEventV2): string | undefined {
    // 1) If a specific route exists (e.g., /races/{raceId})
    const fromParam = e.pathParameters?.["raceId"];
    if (fromParam) return fromParam;
  
    // 2) Catch-all route: parse from proxy ("races/race2")
    const proxy = e.pathParameters?.["proxy"];
    if (proxy) {
      const m = proxy.match(/^races\/([^/]+)/);
      if (m) return m[1];
    }
  
    // 3) Fallback: parse from rawPath ("/races/race2")
    if (e.rawPath) {
      const m = e.rawPath.match(/\/races\/([^/]+)/);
      if (m) return m[1];
    }
  
    return undefined;
}

export const racesRouter = async (e: APIGatewayProxyEventV2): Promise<APIGatewayProxyResultV2> => {
  const method = e.requestContext.http.method;
  const raceId = getRaceId(e);

  if (method === "GET" && !raceId) {
    const res = await ddb.send(new ScanCommand({ TableName: RACES }));
    const items = (res.Items ?? [])
      .map(i => normalizeRace(i as any))
      .sort((a, b) => String(a.date).localeCompare(String(b.date)));
    return { statusCode: 200, body: JSON.stringify(items) };
  }

  if (method === "GET" && raceId) {
    const res = await ddb.send(new GetCommand({ TableName: RACES, Key: { raceId } }));
    if (!res.Item) return { statusCode: 404, body: JSON.stringify({ error: "Race not found" }) };
    return { statusCode: 200, body: JSON.stringify(normalizeRace(res.Item as any) )};
  }

  if (method === "PATCH" && raceId) {
    const body = JSON.parse(e.body || "{}");
    const { locked, independent, name, location, date, type } = body as {
      locked?: boolean;
      independent?: boolean;
      name?: string;
      location?: string;
      date?: string;
      type?: string;
    };
    if ([locked, independent, name, location, date, type].every(v => v === undefined)) {
      return { statusCode: 400, body: JSON.stringify({ error: "No fields to update" }) };
    }
    if (locked !== undefined && typeof locked !== "boolean") {
      return { statusCode: 400, body: JSON.stringify({ error: "locked must be a boolean" }) };
    }
    if (independent !== undefined && typeof independent !== "boolean") {
      return { statusCode: 400, body: JSON.stringify({ error: "independent must be a boolean" }) };
    }
    if (name !== undefined && (typeof name !== "string" || !name.trim())) {
      return { statusCode: 400, body: JSON.stringify({ error: "name must be a non-empty string" }) };
    }
    if (location !== undefined && (typeof location !== "string" || !location.trim())) {
      return { statusCode: 400, body: JSON.stringify({ error: "location must be a non-empty string" }) };
    }
    if (date !== undefined) {
      const validDate = typeof date === "string" && /^\d{4}-\d{2}-\d{2}$/.test(date);
      if (!validDate) return { statusCode: 400, body: JSON.stringify({ error: "date must be YYYY-MM-DD" }) };
    }
    if (type !== undefined && !RACE_TYPES.has(type)) {
      return { statusCode: 400, body: JSON.stringify({ error: "type must be Slalom or Giant Slalom" }) };
    }

    const existing = await ddb.send(new GetCommand({ TableName: RACES, Key: { raceId } }));
    if (!existing.Item) return { statusCode: 404, body: JSON.stringify({ error: "Race not found" }) };

    const updates: string[] = [];
    const values: Record<string, any> = {};
    const names: Record<string, string> = {};
    if (locked !== undefined) { updates.push("locked = :l"); values[":l"] = locked; }
    if (independent !== undefined) { updates.push("independent = :i"); values[":i"] = independent; }
    if (name !== undefined) { updates.push("#name = :n"); values[":n"] = name.trim(); names["#name"] = "name"; }
    if (location !== undefined) { updates.push("#location = :loc"); values[":loc"] = location.trim(); names["#location"] = "location"; }
    if (date !== undefined) { updates.push("#date = :d"); values[":d"] = date; names["#date"] = "date"; }
    if (type !== undefined) { updates.push("#type = :t"); values[":t"] = type; names["#type"] = "type"; }

    if (updates.length === 0) {
      return { statusCode: 400, body: JSON.stringify({ error: "No valid fields to update" }) };
    }

    const updated = await ddb.send(new UpdateCommand({
      TableName: RACES,
      Key: { raceId },
      UpdateExpression: `SET ${updates.join(", ")}`,
      ExpressionAttributeValues: values,
      ExpressionAttributeNames: Object.keys(names).length ? names : undefined,
      ReturnValues: "ALL_NEW",
      ConditionExpression: "attribute_exists(raceId)",
    }));
    const item = updated.Attributes ?? { ...existing.Item, locked: body.locked };
    return { statusCode: 200, body: JSON.stringify(normalizeRace(item as any)) };
  }

  return {  statusCode: 404,  body: JSON.stringify({ error: "Not found" }) };
};
