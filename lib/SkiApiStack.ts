import { Stack, StackProps, Duration, CfnOutput } from "aws-cdk-lib";
import { Construct } from "constructs";
import { AttributeType, BillingMode, Table, ProjectionType } from "aws-cdk-lib/aws-dynamodb";
import { Runtime, Code, Function as LambdaFn } from "aws-cdk-lib/aws-lambda";
import { HttpApi, CorsHttpMethod, HttpMethod, HttpRoute, HttpRouteKey, HttpIntegrationSubtype } from "aws-cdk-lib/aws-apigatewayv2";
import { HttpLambdaIntegration as V2Integration } from "aws-cdk-lib/aws-apigatewayv2-integrations";


export class SkiApiStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, 
      { ...props,
        env:
        {
          account: "794614377434",
          region: "us-east-2"
        },
    });

    // // Single table design (PK + SK) to keep it simple & cheap
    // const table = new dynamodb.Table(this, "SkiTable", {
    //   partitionKey: { name: "pk", type: dynamodb.AttributeType.STRING }, // e.g. "RACER#123"
    //   sortKey: { name: "sk", type: dynamodb.AttributeType.STRING },      // e.g. "META"
    //   billingMode: dynamodb.BillingMode.PAY_PER_REQUEST, // fits free tier well
    //   removalPolicy: /* NOT for prod */ 0 as any, // keep default, or use RemovalPolicy.RETAIN
    // });

    const teams = new Table(this, "Teams", {
      tableName: "Teams",
      partitionKey: { name: "teamId", type: AttributeType.STRING },
      billingMode: BillingMode.PAY_PER_REQUEST,
    });

    const racers = new Table(this, "Racers", {
      tableName: "Racers",
      partitionKey: { name: "racerId", type: AttributeType.STRING },
      billingMode: BillingMode.PAY_PER_REQUEST,
    });
    racers.addGlobalSecondaryIndex({
      indexName: "byTeam",
      partitionKey: { name: "teamId", type: AttributeType.STRING },
      projectionType: ProjectionType.ALL,
    });

    const races = new Table(this, "Races", {
      tableName: "Races",
      partitionKey: { name: "raceId", type: AttributeType.STRING },
      billingMode: BillingMode.PAY_PER_REQUEST,
    });

    const rosters = new Table(this, "Rosters", {
      tableName: "Rosters",
      partitionKey: { name: "pk", type: AttributeType.STRING },
      sortKey: { name: "sk", type: AttributeType.STRING },
      billingMode: BillingMode.PAY_PER_REQUEST,
    });

    const startLists = new Table(this, "StartLists", {
      tableName: "StartLists",
      partitionKey: { name: "raceId", type: AttributeType.STRING },
      sortKey: { name: "bib", type: AttributeType.NUMBER },
      billingMode: BillingMode.PAY_PER_REQUEST,
    });

    // Lambda (monolith handler with tiny router)
    const apiFn = new LambdaFn(this, "ApiFn", {
      runtime: Runtime.NODEJS_20_X,
      memorySize: 256,
      timeout: Duration.seconds(10),
      code: Code.fromAsset("./src/"), // built JS output
      handler: "index.handler",
      environment: {
        TEAMS_TABLE: teams.tableName,
        RACERS_TABLE: racers.tableName,
        RACES_TABLE: races.tableName,
        ROSTERS_TABLE: rosters.tableName,
        STARTLISTS_TABLE: startLists.tableName,
      },
    });

    teams.grantReadWriteData(apiFn);
    racers.grantReadWriteData(apiFn);
    races.grantReadWriteData(apiFn);
    rosters.grantReadWriteData(apiFn);
    startLists.grantReadWriteData(apiFn);

    const api = new HttpApi(this, "RaceManagerApi", {
      corsPreflight: {
        allowHeaders: ["*"],
        allowMethods: [CorsHttpMethod.ANY],
        allowOrigins: ["*"], // tighten later
      },
    });

    const integration = new V2Integration("ApiIntegration", apiFn);

    // Route all to Lambda (the handler routes by path/method)
    // api.addRoutes({
    //   path: "/{proxy+}",
    //   integration,
    //   methods: [HttpMethod.ANY],
    // });
    api.addRoutes(
    {
      path: "/races/{raceId}",
          integration,
          methods: [HttpMethod.GET]
    
    });
    api.addRoutes(
    {
      path: "/teams/{teamId}",
          integration,
          methods: [HttpMethod.GET]
    
    });

    new CfnOutput(this, "ApiUrl", { value: api.apiEndpoint });

    // Output the base URL after deploy
    this.exportValue(api.apiEndpoint, { name: "ApiEndpoint" });
  }
}
