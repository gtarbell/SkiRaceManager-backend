import { APIGatewayProxyEventV2, APIGatewayProxyResultV2 } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, GetCommand, ScanCommand, QueryCommand } from "@aws-sdk/lib-dynamodb";

const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const TEAMS = process.env.TEAMS_TABLE!;
const RACERS = process.env.RACERS_TABLE!;

async function getTeam(teamId: string) {
  const t = await ddb.send(new GetCommand({ TableName: TEAMS, Key: { teamId } }));
  if (!t.Item) return null;

  // get racers for team
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

export const teamsRouter = async (e: APIGatewayProxyEventV2): Promise<APIGatewayProxyResultV2> => {
  const method = e.requestContext.http.method;
  const params = e.pathParameters ?? {};
  const teamId = params["teamId"];

  if (method === "GET" && !teamId) {
    // Optional filter: /teams?ids=a,b,c
    const idsParam = e.queryStringParameters?.ids;
    if (idsParam) {
      const ids = idsParam.split(",").map(s => s.trim()).filter(Boolean);
      const results = await Promise.all(ids.map(id => getTeam(id)));
      return { statusCode: 200, body: JSON.stringify(results.filter(Boolean)) };
    }
    // otherwise, all teams (with racers for convenience of current UI)
    const scan = await ddb.send(new ScanCommand({ TableName: TEAMS }));
    const teams = await Promise.all((scan.Items ?? []).map(t => getTeam(t.teamId)));
    return { statusCode: 200, body: JSON.stringify(teams.filter(Boolean)) };
  }

  if (method === "GET" && teamId) {
    const t = await getTeam(teamId);
    if (!t) return { statusCode: 404, body: JSON.stringify({ error: "Team not found" } )};
    return { statusCode: 200, body: JSON.stringify(t) };
  }

  return { statusCode: 404, body: JSON.stringify({ error: "Not found" }) };
};
