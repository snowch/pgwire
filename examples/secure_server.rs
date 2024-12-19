use std::sync::Arc;

use async_trait::async_trait;
use futures::{stream, StreamExt};
use tokio::net::TcpListener;

use colored::Colorize;

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::logging::LoggingStream;
use pgwire::api::portal::Portal;
use pgwire::api::query::{SimpleQueryHandler, ExtendedQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldInfo, FieldFormat, QueryResponse,
    Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{ClientInfo, NoopErrorHandler, PgWireServerHandlers, Type};
use pgwire::error::PgWireResult;
use pgwire::tokio::process_socket;

pub struct DummyProcessor {
    // conn: Arc<Mutex<Connection>>,
    query_parser: Arc<NoopQueryParser>,
}

impl DummyProcessor {
    fn new() -> DummyProcessor {
        DummyProcessor {
            // conn: Arc::new(Mutex::new(Connection::open_in_memory().unwrap())),
            query_parser: Arc::new(NoopQueryParser::new()),
        }
    }
}

impl NoopStartupHandler for DummyProcessor {}

#[async_trait]
impl SimpleQueryHandler for DummyProcessor {
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        println!("SimpleQueryHandler.do_query | {:?}", query);

        if query.starts_with("SELECT") {
            let f1 = FieldInfo::new("id".into(), None, None, Type::INT4, FieldFormat::Text);
            let f2 = FieldInfo::new("name".into(), None, None, Type::VARCHAR, FieldFormat::Text);
            let schema = Arc::new(vec![f1, f2]);

            let data = vec![
                (Some(0), Some("Tom")),
                (Some(1), Some("Jerry")),
                (Some(2), None),
            ];
            let schema_ref = schema.clone();
            let data_row_stream = stream::iter(data.into_iter()).map(move |r| {
                let mut encoder = DataRowEncoder::new(schema_ref.clone());
                encoder.encode_field(&r.0)?;
                encoder.encode_field(&r.1)?;

                encoder.finish()
            });

            Ok(vec![Response::Query(QueryResponse::new(
                schema,
                data_row_stream,
            ))])
        } else {
            Ok(vec![Response::Execution(Tag::new("OK").with_rows(1))])
        }
    }
}

#[async_trait]
impl ExtendedQueryHandler for DummyProcessor {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<'a, 'b: 'a, C>(
        &'b self,
        _client: &mut C,
        _portal: &'a Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let t = "ExtendedQueryHandler.do_query".bold();
        println!("{} | {}", t, &_portal.statement.statement);

        let f1 = FieldInfo::new("id".into(), None, None, Type::INT4, FieldFormat::Text);
        let f2 = FieldInfo::new("name".into(), None, None, Type::VARCHAR, FieldFormat::Text);
        let schema = Arc::new(vec![f1, f2]);

        let data = vec![
            (Some(0), Some("Tom")),
            (Some(1), Some("Jerry")),
            (Some(2), None),
        ];
        let schema_ref = schema.clone();
        let data_row_stream = stream::iter(data.into_iter()).map(move |r| {
            let mut encoder = DataRowEncoder::new(schema_ref.clone());
            encoder.encode_field(&r.0)?;
            encoder.encode_field(&r.1)?;

            encoder.finish()
        });

        Ok(Response::Query(QueryResponse::new(
            schema,
            data_row_stream,
        )))
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        _statement: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        unimplemented!("do_describe_statement | Extended Query is not implemented on this server.")
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        _portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let t = "ExtendedQueryHandler.do_describe_portal".bold();
        println!("{} | {}", t, &_portal.statement.statement);
        
        let f1 = FieldInfo::new("id".into(), None, None, Type::INT4, FieldFormat::Text);
        let f2 = FieldInfo::new("name".into(), None, None, Type::VARCHAR, FieldFormat::Text);
        // let schema = Arc::new(vec![f1, f2]);

        Ok(DescribePortalResponse::new(vec![f1, f2]))
    }
}

struct DummyProcessorFactory {
    handler: Arc<DummyProcessor>,
}

impl PgWireServerHandlers for DummyProcessorFactory {
    type StartupHandler = DummyProcessor;
    type SimpleQueryHandler = DummyProcessor;
    type ExtendedQueryHandler = DummyProcessor;
    type CopyHandler = NoopCopyHandler;
    type ErrorHandler = NoopErrorHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        self.handler.clone()
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        self.handler.clone()
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }

    fn error_handler(&self) -> Arc<Self::ErrorHandler> {
        Arc::new(NoopErrorHandler)
    }
}

#[tokio::main]
pub async fn main() {
    let factory = Arc::new(DummyProcessorFactory {
        handler: Arc::new(DummyProcessor::new()),
    });

    let server_addr = "0.0.0.0:5432";
    let listener = TcpListener::bind(server_addr).await.unwrap();

    println!("Listening to {}", server_addr);
    loop {
        let (socket, _) = listener.accept().await.unwrap();
        let factory_ref = factory.clone();

        // Wrap socket with LoggingStream
        let logging_socket = LoggingStream::new(socket);

        tokio::spawn(async move {
            let _ = process_socket(logging_socket.into_inner(), None, factory_ref).await;
        });
    }
}
