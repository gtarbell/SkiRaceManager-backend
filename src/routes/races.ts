import { APIGatewayProxyEventV2, APIGatewayProxyResultV2 } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, ScanCommand, GetCommand, UpdateCommand } from "@aws-sdk/lib-dynamodb";

const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const RACES = process.env.RACES_TABLE!;

function normalizeRace<T extends Record<string, any>>(raw: T): T & { locked: boolean } {
  return { ...raw, locked: Boolean(raw.locked) };
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
    if (typeof body.locked !== "boolean") {
      return { statusCode: 400, body: JSON.stringify({ error: "locked (boolean) is required" }) };
    }

    const existing = await ddb.send(new GetCommand({ TableName: RACES, Key: { raceId } }));
    if (!existing.Item) return { statusCode: 404, body: JSON.stringify({ error: "Race not found" }) };

    const updated = await ddb.send(new UpdateCommand({
      TableName: RACES,
      Key: { raceId },
      UpdateExpression: "SET locked = :l",
      ExpressionAttributeValues: { ":l": body.locked },
      ReturnValues: "ALL_NEW",
      ConditionExpression: "attribute_exists(raceId)",
    }));
    const item = updated.Attributes ?? { ...existing.Item, locked: body.locked };
    return { statusCode: 200, body: JSON.stringify(normalizeRace(item as any)) };
  }

  return {  statusCode: 404,  body: JSON.stringify({ error: "Not found" }) };
};
