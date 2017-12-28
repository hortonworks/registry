<h4>Steps to try out custom schema lifecycle example</h4>
  * Run *SchemaLifecycleApp* which takes *resources/registry-lifecycle-example.yaml* as it's configuration. This class will kick start Schema Registry service and will incorporate a custom schema lifecycle as defined in *CustomReviewCycleExecutor*.
  * Run *review/service/ReviewServiceApp* which takes *resources/review-service.yaml* as it's configuration. This class will be used to make custom state transitions outside of Schema Registry.
  * Open a web browser and navigate to Schema Registry url.
  * Register a new schema and create a new branch from the registered schema.
  * In the schema branch created in the previous step, add a new schema version. The created schema version will be in 'INITIATED' state.
  * Change the status of the schema version in the previous step to 'StartReview', the UI will transition to 'PeerReview' state.
  * Use the ReviewService API *POST /v1/review/peer/schema/{versionId}/[accept | reject | modify]* to change the status of schema version to 'TechnicalLeadReview', 'Rejected' and 'ChangesRequired' state respectively.
  * Once the schema version has moved to 'TechnicalLeadReview' you can use *POST /v1/review/technical/schema/{versionId}/[accept | reject | modify]* to change the status of schema version to 'ReviewedState', 'Rejected' and 'ChangesRequired' state respectively.
  * If the schema version is moved to 'ReviewedState', it can then be enabled from the UI. Enabling a schema version would trigger the custom listener and post a notification to the Review Service, which should print below message in the Review Service log.
         <pre>                            &lt;&lt;==== Schema version info : [0-9]+ has transitioned to enabled state ====&gt;&gt; </pre>