#[cfg(feature = "otel")]
pub fn init_otel_from_env() {
    use opentelemetry_otlp::WithExportConfig;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4317".to_string());
    let service_name = std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "hyperzoekt".into());

    let exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint(endpoint);
    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(exporter)
        .install_batch(opentelemetry_sdk::runtime::Tokio)
        .expect("install OTLP pipeline");

    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    let fmt_layer = tracing_subscriber::fmt::layer();
    let filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        // Default: app info; quiet noisy deps, tracing internals, and ignore crate
        // Specifically silence the noisy connection pooling module used during tests.
        "info,hyper_util::client::legacy::pool=warn,hyper_util=warn,hyper=warn,h2=warn,reqwest=warn,tower_http=warn,tracing=warn,ignore=warn".into()
    });
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .with(otel_layer)
        .init();

    // Set W3C Propagator
    let _ = opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );
    // Set default service name env var for downstream tools that read it
    let _ = std::env::set_var("OTEL_SERVICE_NAME", service_name);
}

#[cfg(not(feature = "otel"))]
pub fn init_otel_from_env() {}
