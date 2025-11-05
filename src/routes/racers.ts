import { APIGatewayProxyEventV2, APIGatewayProxyResultV2 } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand, UpdateCommand, DeleteCommand, QueryCommand, GetCommand } from "@aws-sdk/lib-dynamodb";
import { nanoid } from "nanoid";

const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const TEAMS = process.env.TEAMS_TABLE!;
const RACERS = process.env.RACERS_TABLE!;
const ROSTERS = process.env.ROSTERS_TABLE!;

// Convenience: return the team + racers (to satisfy current UI expectations)
async function loadTeamWithRacers(teamId: string) {
  const t = await ddb.send(new GetCommand({ TableName: TEAMS, Key: { teamId } }));
  if (!t.Item) return null;
  const r = await ddb.send(
    new QueryCommand({
      TableName: RACERS,
      IndexName: "byTeam",
      KeyConditionExpression: "teamId = :t",
      ExpressionAttributeValues: { ":t": teamId },
    })
  );
  return { ...t.Item, racers: r.Items ?? [] };
}

export const racersRouter = async (e: APIGatewayProxyEventV2): Promise<APIGatewayProxyResultV2> => {
  const method = e.requestContext.http.method;
  const params = e.pathParameters ?? {};
  const teamId = params["teamId"]!;
  const racerId = params["racerId"];
  const body = e.body ? JSON.parse(e.body) : null;

  if (method === "POST" && e.rawPath.endsWith(`/teams/${teamId}/racers`)) {
    const { name, gender, class: racerClass } = body ?? {};
    const id = nanoid(10);
    await ddb.send(new PutCommand({
      TableName: RACERS,
      Item: { racerId: id, teamId, name, gender, class: racerClass },
    }));
    const team = await loadTeamWithRacers(teamId);
    return { statusCode: 200, body: JSON.stringify(team )};
  }

  if (method === "PATCH" && racerId) {
    const { name, gender, class: racerClass } = body ?? {};
    const expr: string[] = [];
    const names: Record<string, string> = {};
    const values: Record<string, any> = {};
    if (name !== undefined) { expr.push("#n = :n"); names["#n"] = "name"; values[":n"] = name; }
    if (gender !== undefined) { expr.push("#g = :g"); names["#g"] = "gender"; values[":g"] = gender; }
    if (racerClass !== undefined) { expr.push("#c = :c"); names["#c"] = "class"; values[":c"] = racerClass; }

    if (expr.length) {
      await ddb.send(new UpdateCommand({
        TableName: RACERS,
        Key: { racerId },
        UpdateExpression: "SET " + expr.join(", "),
        ExpressionAttributeNames: names,
        ExpressionAttributeValues: values,
      }));
    }
    const team = await loadTeamWithRacers(teamId);
    return { statusCode: 200, body: JSON.stringify(team )};
  }

  if (method === "DELETE" && racerId) {
    // Remove from racers
    await ddb.send(new DeleteCommand({ TableName: RACERS, Key: { racerId } }));
    // Also remove from all rosters for this team (best-effort)
    // Query all race/team roster items containing this racerId
    // (We don't know all raceIds here; in a real app you'd keep a GSI or do a scan with filter. For now, best-effort leave as-is or add a TODO.)
    // TODO: add a GSI on ROSTERS for racerId to delete across races quickly.

    const team = await loadTeamWithRacers(teamId);
    return { statusCode: 200, body: JSON.stringify(team )};
  }

  return { statusCode: 404, body: JSON.stringify({ error: "Not found" } )};
};
