-- Copyright (c) 2026 ADBC Drivers Contributors
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--         http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

INSTALL COMPONENT 'file://component_query_attributes';

INSTALL COMPONENT 'file://component_telemetry'
SET
    PERSIST telemetry.log_enabled = ON,
    PERSIST telemetry.metrics_enabled = ON,
    PERSIST telemetry.trace_enabled = ON,
    PERSIST telemetry.query_text_enabled = ON,
    PERSIST telemetry.otel_exporter_otlp_logs_endpoint = 'http://otel-collector:4318/v1/logs',
    PERSIST telemetry.otel_exporter_otlp_logs_protocol = 'http/protobuf',
    PERSIST telemetry.otel_exporter_otlp_metrics_endpoint = 'http://otel-collector:4318/v1/metrics',
    PERSIST telemetry.otel_exporter_otlp_metrics_protocol = 'http/protobuf',
    PERSIST telemetry.otel_exporter_otlp_traces_endpoint = 'http://otel-collector:4318/v1/traces',
    PERSIST telemetry.otel_exporter_otlp_traces_protocol = 'http/protobuf',
    PERSIST telemetry.otel_resource_attributes = 'service.name=mysql';
