# AWS Lambda Deployment

## Prerequisites
- AWS account with permissions to manage Lambda, IAM, and S3.
- Rust stable toolchain and `cargo lambda` (`cargo install cargo-lambda`).
- Configured S3 bucket `<S3_BUCKET>` in `<REGION>` for Parquet files.
- AWS CLI configured with deployment credentials.

## Build
1. **Install Lambda target toolchain.**
   ```bash
   rustup target add x86_64-unknown-linux-musl
   ```
   Expected output: `info: downloading component 'rust-std' for 'x86_64-unknown-linux-musl'`.
2. **Build the Lambda bundle.**
   ```bash
   cargo lambda build --release --features lambda
   ```
   Expected output: `Finished release [optimized] target(s) in ...` and `Packaged bootstrap.zip`.

## Configure
1. **Create execution role with least privilege.**
   ```bash
   aws iam create-role --role-name otlp2parquet-lambda \
     --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
   ```
   Expected output: Role ARN for the Lambda function.
2. **Attach policies for logging and S3 writes.**
   ```bash
   aws iam attach-role-policy --role-name otlp2parquet-lambda --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
   aws iam put-role-policy --role-name otlp2parquet-lambda --policy-name otlp2parquet-write \
     --policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:PutObject","s3:AbortMultipartUpload"],"Resource":"arn:aws:s3:::<S3_BUCKET>/*"}]}'
   ```
   Expected output: `PutRolePolicy` success message.

## Deploy
1. **Create or update the Lambda function.**
   ```bash
   aws lambda create-function \
     --function-name otlp2parquet-<ENV> \
     --runtime provided.al2 \
     --role arn:aws:iam::<ACCOUNT_ID>:role/otlp2parquet-lambda \
     --handler bootstrap \
     --zip-file fileb://target/lambda/otlp2parquet/bootstrap.zip \
     --timeout 30 \
     --memory-size 256 \
     --environment "Variables={S3_BUCKET=<S3_BUCKET>,S3_REGION=<REGION>,STORAGE_BACKEND=s3}"
   ```
   Expected output: JSON response with `FunctionArn`.
2. **Provision an HTTPS endpoint with Function URL (optional).**
   ```bash
   aws lambda create-function-url-config --function-name otlp2parquet-<ENV> --auth-type NONE
   ```
   Expected output: URL ending in `.lambda-url.<REGION>.on.aws`.

## Verify
1. **Invoke with a sample payload.**
   ```bash
   aws lambda invoke --function-name otlp2parquet-<ENV> \
     --payload '{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"lambda-demo"}}]},"scopeLogs":[{"logRecords":[{"timeUnixNano":"1710000000000000000","severityText":"INFO","body":{"stringValue":"Lambda log"}}]}]}]}' \
     response.json
   ```
   Expected output: `StatusCode: 200` and `FunctionError: null`.
2. **Check S3 bucket for new objects.**
   ```bash
   aws s3 ls s3://<S3_BUCKET>/logs/lambda-demo/ --recursive --summarize
   ```
   Expected output: Listing shows one Parquet object.

Now you should have otlp2parquet running at the Lambda Function URL for otlp2parquet-<ENV>.

## Rollback
1. **Publish a function version.**
   ```bash
   aws lambda publish-version --function-name otlp2parquet-<ENV>
   ```
   Expected output: JSON with `Version` identifier.
2. **Repoint alias to previous version.**
   ```bash
   aws lambda update-alias --function-name otlp2parquet-<ENV> --name live --function-version <PREVIOUS_VERSION>
   ```
   Expected output: Alias updated to `<PREVIOUS_VERSION>`.

## Performance & Cost Tips
- Enable provisioned concurrency for sub-second cold starts on critical paths.
- Keep `timeout` under 30 seconds to avoid unnecessary billing granularity.
- Use `S3_PREFIX` to partition data by tenant and reduce LIST overhead.
- Monitor `Duration` and `InitDuration` metrics; recompile with `RUSTFLAGS="-C strip=symbols"` to reduce init time.
