#[tokio::test]
async fn test_s3_object_store() {
    use vdb2::object_store::s3::S3Store;
    use aws_config;
    use aws_sdk_s3::Client;

    // Set this in your environment for the test
    let bucket = std::env::var("TEST_S3_BUCKET").expect("TEST_S3_BUCKET not set");
    let config = aws_config::load_from_env().await;
    let client = Client::new(&config);
    let store = S3Store::new(bucket, client);

    // Use a unique prefix for isolation
    let prefix = format!("test/{}/", uuid::Uuid::new_v4());
    vdb2::object_store::test_helpers::tests::run_object_store_tests(&store, &prefix);
}
