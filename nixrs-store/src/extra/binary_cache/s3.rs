pub fn default_buffer_size() -> u64 {
    5 * 1024 * 1024
}

struct XmlError {
    message: String,
    code: String,
    resource: String,
    request_id: String,
}

pub struct S3StoreError {
    msg: String,
    source: RusotoError,
}

pub struct S3Config {
    parent: super::Config,

    //const Setting<std::string> profile{(StoreConfig*) this, "", "profile", "The name of the AWS configuration profile to use."};
    /// The name of the AWS configuration profile to use.
    #[serde(default)]
    profile: Option<String>,

    //const Setting<std::string> region{(StoreConfig*) this, Aws::Region::US_EAST_1, "region", {"aws-region"}};
    region: String,

    //const Setting<std::string> scheme{(StoreConfig*) this, "", "scheme", "The scheme to use for S3 requests, https by default."};
    /// The scheme to use for S3 requests, https by default.
    #[serde(default)]
    scheme: Option<String>,

    //const Setting<std::string> endpoint{(StoreConfig*) this, "", "endpoint", "An optional override of the endpoint to use when talking to S3."};
    /// An optional override of the endpoint to use when talking to S3.
    #[serde(default)]
    endpoint: Option<String>,

    //const Setting<std::string> narinfoCompression{(StoreConfig*) this, "", "narinfo-compression", "compression method for .narinfo files"};
    /// compression method for .narinfo files
    #[serde(rename = "narinfo-compression")]
    #[serde(default)]
    narinfo_compression: Option<Compression>,

    //const Setting<std::string> lsCompression{(StoreConfig*) this, "", "ls-compression", "compression method for .ls files"};
    /// compression method for .ls files
    #[serde(rename = "ls-compression")]
    #[serde(default)]
    ls_compression: Option<Compression>,

    //const Setting<std::string> logCompression{(StoreConfig*) this, "", "log-compression", "compression method for log/* files"};
    /// compression method for log/* files
    #[serde(rename = "log-compression")]
    #[serde(default)]
    log_compression: Option<Compression>,

    //const Setting<bool> multipartUpload{
    //    (StoreConfig*) this, false, "multipart-upload", "whether to use multi-part uploads"};
    /// whether to use multi-part uploads
    #[serde(rename = "multipart-upload")]
    #[serde(default)]
    multipart_upload: bool,

    //const Setting<uint64_t> bufferSize{
    //    (StoreConfig*) this, 5 * 1024 * 1024, "buffer-size", "size (in bytes) of each part in multi-part uploads"};
    /// size (in bytes) of each part in multi-part uploads
    #[serde(rename = "buffer-size")]
    #[serde(default = "default_buffer_size")]
    buffer_size: u64,
    //const std::string name() override { return "S3 Binary Cache Store"; }
}

struct S3BinaryCache {
    config: S3Config,
    bucket_name: String,
}

impl BinaryCache for S3BinaryCache {
    fn uri_schemes(&self) -> HashSet<String> {
        let r = HashSet::new();
        r.insert("s3".to_string());
        r
    }

    fn file_exists(&self, path: &str) -> Result<bool, Error> {
        /*
        auto res = s3Helper.client->HeadObject(
            Aws::S3::Model::HeadObjectRequest()
            .WithBucket(bucketName)
            .WithKey(path));

        if (!res.IsSuccess()) {
            auto & error = res.GetError();
            if (error.GetErrorType() == Aws::S3::S3Errors::RESOURCE_NOT_FOUND
                || error.GetErrorType() == Aws::S3::S3Errors::NO_SUCH_KEY
                // If bucket listing is disabled, 404s turn into 403s
                || error.GetErrorType() == Aws::S3::S3Errors::ACCESS_DENIED)
                return false;
            throw Error("AWS error fetching '%s': %s", path, error.GetMessage());
        }

        return true;
        */

        let req: HeadObjectRequest = Default::default();
        req.bucket = self.bucket_name.clone();
        req.key = path.to_owned();
        let res = self.client.head_object(req).await;
        if let Err(err) = res {
            match err {
                RusotoError::Service(HeadObjectError::NoSuchKey(_)) => Ok(false),
                RusotoError::Unknown(u) if u.status == StatusCode::FORBIDDEN => Ok(false),
                _ => Err(Error::format("AWS error fetching '{}': {}", path)),
            }
        } else {
            Ok(true)
        }
    }

    fn upsert_file<R: Read>(&self, path: &StorePath, stream: R, mime_type: &str) {}

    fn upsert_file(&self, path: &StorePath, data: &[u8], mime_type: &str) {
        let stream = Cursor::new(data);
        self.upsert_file(path, stream, mime_type)
    }

    /* Dump the contents of the specified file to a sink. */
    fn get_file(&self, path: &StorePath) -> Vec<u8> {}

    fn query_all_valid_paths(&self) -> HashSet<StorePath> {}
}
