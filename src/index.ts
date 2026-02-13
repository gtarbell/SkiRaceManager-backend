import { APIGatewayProxyEventV2, APIGatewayProxyResultV2, APIGatewayProxyStructuredResultV2 } from "aws-lambda";
import { teamsRouter } from "./routes/teams";
import { racersRouter } from "./routes/racers";
import { rosterRouter } from "./routes/roster";
import { racesRouter } from "./routes/races"
import { startlistRouter } from "./routes/startlist";
import { resultsRouter } from "./routes/results";

type Route = (e: APIGatewayProxyEventV2) => Promise<APIGatewayProxyResultV2>;

const notFound: Route = async () => ({ statusCode: 404, body: "Not found" });
const corsHeaders = {
  "content-type": "application/json",
  "access-control-allow-origin": "*",
  "access-control-allow-headers": "content-type,authorization",
  "access-control-allow-methods": "GET,POST,PATCH,DELETE,OPTIONS",
};

const routes: Record<string, Route> = {
  "GET /teams": teamsRouter,
  "GET /teams/{teamId}": teamsRouter,
  "PATCH /teams/{teamId}": teamsRouter,
  "POST /teams/{teamId}/racers": racersRouter,
  "PATCH /teams/{teamId}/racers/{racerId}": racersRouter,
  "DELETE /teams/{teamId}/racers/{racerId}": racersRouter,

  "GET /races": racesRouter,
  "POST /races": racesRouter,
  "GET /races/{raceId}": racesRouter,
  "DELETE /races/{raceId}": racesRouter,
  "PATCH /races/{raceId}": racesRouter,
  "POST /races/roster-counts": rosterRouter,

  "GET /races/{raceId}/roster/{teamId}": rosterRouter,
  "POST /races/{raceId}/roster/{teamId}/add": rosterRouter,
  "PATCH /races/{raceId}/roster/{teamId}/entry/{racerId}": rosterRouter,
  "POST /races/{raceId}/roster/{teamId}/move": rosterRouter,
  "DELETE /races/{raceId}/roster/{teamId}/entry/{racerId}": rosterRouter,
  "POST /races/{raceId}/roster/{teamId}/copy": rosterRouter,
  "POST /races/{raceId}/start-list/generate": startlistRouter,
  "POST /races/{raceId}/start-list/copy": startlistRouter,
  "GET /races/{raceId}/start-list": startlistRouter,
  "GET /races/{raceId}/start-list/excluded": startlistRouter,
  "POST /races/{raceId}/start-list/excluded": startlistRouter,

  "GET /races/{raceId}/results": resultsRouter,
  "POST /races/{raceId}/results": resultsRouter,
  "POST /races/{raceId}/results/recalc": resultsRouter,
};

function keyOf(e: APIGatewayProxyEventV2) {
  const p = e.rawPath.replace(/\/+$/, "");
  const m = e.requestContext.http.method.toUpperCase();
  // normalize dynamic parts into templates (quick & dirty)
  const norm = p
    .replace(/\/[^/]+\/racers\/[^/]+$/, "/{teamId}/racers/{racerId}")
    .replace(/\/[^/]+\/racers$/, "/{teamId}/racers")
    .replace(/\/races\/[^/]+\/roster\/[^/]+\/entry\/[^/]+$/, "/races/{raceId}/roster/{teamId}/entry/{racerId}")
    .replace(/\/races\/[^/]+\/roster\/[^/]+\/move$/, "/races/{raceId}/roster/{teamId}/move")
    .replace(/\/races\/[^/]+\/roster\/[^/]+\/add$/, "/races/{raceId}/roster/{teamId}/add")
    .replace(/\/races\/[^/]+\/roster\/[^/]+\/copy$/, "/races/{raceId}/roster/{teamId}/copy")
    .replace(/\/races\/roster-counts$/, "/races/roster-counts")
    .replace(/\/races\/[^/]+\/start-list\/generate$/, "/races/{raceId}/start-list/generate")
    .replace(/\/races\/[^/]+\/start-list\/copy$/, "/races/{raceId}/start-list/copy")
    .replace(/\/races\/[^/]+\/start-list\/excluded$/, "/races/{raceId}/start-list/excluded")
    .replace(/\/races\/[^/]+\/start-list$/, "/races/{raceId}/start-list")
    .replace(/\/races\/[^/]+\/results$/, "/races/{raceId}/results")
    .replace(/\/races\/[^/]+\/results\/recalc$/, "/races/{raceId}/results/recalc")
    .replace(/\/races\/[^/]+\/roster\/[^/]+$/, "/races/{raceId}/roster/{teamId}")
    .replace(/\/races\/[^/]+\/start-list\/generate$/, "/races/{raceId}/start-list/generate")
    .replace(/\/races\/[^/]+\/start-list$/, "/races/{raceId}/start-list")
    .replace(/\/races\/(?!roster-counts$)[^/]+$/, "/races/{raceId}")
    .replace(/\/teams\/[^/]+$/, "/teams/{teamId}");
  return `${m} ${norm || "/"}`;
}



export const handler = async (e: APIGatewayProxyEventV2) => {
  try {
    if (e.requestContext.http.method.toUpperCase() === "OPTIONS") {
      return { statusCode: 200, headers: corsHeaders, body: "" };
    }
    const k = keyOf(e);
    const fn = routes[k] || notFound;
    const proxyResult = await fn(e) as APIGatewayProxyStructuredResultV2;
    return {
      statusCode: 200,
      headers: corsHeaders,
      body: proxyResult.body
    };
  } catch (err: any) {
    return { statusCode: 500, headers: corsHeaders, body: JSON.stringify({ error: err.message || "Server error" }) };
  }
};
