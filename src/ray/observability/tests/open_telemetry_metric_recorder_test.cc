// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/observability/open_telemetry_metric_recorder.h"

#include "gtest/gtest.h"
#include "opentelemetry/exporters/otlp/otlp_metric_utils.h"
#include "opentelemetry/sdk/metrics/export/metric_producer.h"

namespace ray {
namespace observability {

// A real SDK pull reader makes collection deterministic without a background
// export racing the assertions. Use the same temporality selector as production.
class PullMetricReader : public opentelemetry::sdk::metrics::MetricReader {
 public:
  opentelemetry::sdk::metrics::AggregationTemporality GetAggregationTemporality(
      opentelemetry::sdk::metrics::InstrumentType type) const noexcept override {
    return opentelemetry::exporter::otlp::OtlpMetricUtils::DeltaTemporalitySelector(type);
  }

 private:
  bool OnForceFlush(std::chrono::microseconds) noexcept override {
    return Collect([](opentelemetry::sdk::metrics::ResourceMetrics &) { return true; });
  }
  bool OnShutDown(std::chrono::microseconds) noexcept override { return true; }
};

class OpenTelemetryMetricRecorderTest : public ::testing::Test {
 public:
  OpenTelemetryMetricRecorderTest()
      : recorder_(OpenTelemetryMetricRecorder::GetInstance()) {}

  static void SetUpTestSuite() {
    // Initialize the OpenTelemetryMetricRecorder with a mock endpoint and intervals
    OpenTelemetryMetricRecorder::GetInstance().Start("localhost:1234",
                                                     std::chrono::milliseconds(10000),
                                                     std::chrono::milliseconds(5000));
  }

  static void TearDownTestSuite() {
    // Cleanup if necessary
    OpenTelemetryMetricRecorder::GetInstance().Shutdown();
  }

  std::optional<double> GetObservableMetricValue(
      const std::string &name,
      const absl::flat_hash_map<std::string, std::string> &tags) {
    std::lock_guard<std::mutex> lock(recorder_.mutex_);
    auto it = recorder_.observations_by_name_.find(name);
    if (it == recorder_.observations_by_name_.end()) {
      return std::nullopt;  // Not registered
    }
    auto tag_it = it->second.find(tags);
    if (tag_it != it->second.end()) {
      return tag_it->second;  // Get the value
    }
    return std::nullopt;
  }

  std::shared_ptr<opentelemetry::sdk::metrics::MetricReader> GetMetricReader() {
    std::lock_guard<std::mutex> lock(recorder_.reader_mutex_);
    return recorder_.metric_reader_;
  }

  std::weak_ptr<opentelemetry::sdk::metrics::MeterProvider> GetProvider() {
    std::lock_guard<std::mutex> lock(recorder_.mutex_);
    return recorder_.meter_provider_;
  }

  std::shared_ptr<PullMetricReader> AttachPullReader() {
    std::lock_guard<std::mutex> lock(recorder_.reader_mutex_);
    EXPECT_EQ(recorder_.metric_reader_, nullptr);
    auto reader = std::make_shared<PullMetricReader>();
    recorder_.metric_reader_ = reader;
    recorder_.meter_provider_->AddMetricReader(reader);
    return reader;
  }

  std::map<std::string, opentelemetry::sdk::metrics::MetricData> Collect(
      opentelemetry::sdk::metrics::MetricReader &reader) {
    std::map<std::string, opentelemetry::sdk::metrics::MetricData> metrics;
    EXPECT_TRUE(reader.Collect([&metrics](opentelemetry::sdk::metrics::ResourceMetrics &data) {
      for (const auto &scope : data.scope_metric_data_) {
        for (const auto &metric : scope.metric_data_) {
          metrics.emplace(metric.instrument_descriptor.name_, metric);
        }
      }
      return true;
    }));
    return metrics;
  }

 protected:
  OpenTelemetryMetricRecorder &recorder_;
};

TEST_F(OpenTelemetryMetricRecorderTest, TestGaugeMetric) {
  recorder_.RegisterGaugeMetric("test_metric", "Test metric description");
  recorder_.SetMetricValue("test_metric", {{"tag1", "value1"}}, 42.0);
  // Get a non-empty value of a registered gauge metric and tags
  ASSERT_EQ(GetObservableMetricValue("test_metric", {{"tag1", "value1"}}), 42.0);
  // Get an empty value of a registered gauge metric with unregistered tags
  ASSERT_EQ(GetObservableMetricValue("test_metric", {{"tag1", "value2"}}), std::nullopt);
}

TEST_F(OpenTelemetryMetricRecorderTest, TestCounterMetric) {
  recorder_.RegisterCounterMetric("test_counter", "Test counter description");
  // Check that the counter metric is registered
  ASSERT_TRUE(recorder_.IsMetricRegistered("test_counter"));
}

TEST_F(OpenTelemetryMetricRecorderTest, TestSumMetric) {
  recorder_.RegisterSumMetric("test_sum", "Test sum description");
  // Check that the sum metric is registered
  ASSERT_TRUE(recorder_.IsMetricRegistered("test_sum"));
}

TEST_F(OpenTelemetryMetricRecorderTest, TestHistogramMetric) {
  recorder_.RegisterHistogramMetric(
      "test_histogram", "Test histogram description", {0.0, 10.0, 20.0, 30.0});
  // Check that the histogram metric is registered
  ASSERT_TRUE(recorder_.IsMetricRegistered("test_histogram"));
}

TEST_F(OpenTelemetryMetricRecorderTest, TestRestartStopsEachReader) {
  recorder_.Shutdown();
  recorder_.Shutdown();
  for (int restart = 0; restart < 3; ++restart) {
    recorder_.Start("localhost:1234",
                    std::chrono::milliseconds(10000),
                    std::chrono::milliseconds(5000));
    auto reader = GetMetricReader();
    ASSERT_NE(reader, nullptr);
    EXPECT_FALSE(reader->IsShutdown());
    std::weak_ptr<opentelemetry::sdk::metrics::MetricReader> old_reader = reader;
    auto old_provider = GetProvider();
    recorder_.Shutdown();
    EXPECT_TRUE(reader->IsShutdown());
    reader.reset();
    EXPECT_TRUE(old_reader.expired());
    EXPECT_TRUE(old_provider.expired());
  }
}

TEST_F(OpenTelemetryMetricRecorderTest, TestRestartPreservesMetricsAndReleasesStorage) {
  using namespace opentelemetry::sdk::metrics;
  recorder_.RegisterGaugeMetric("restart_gauge", "Gauge across restarts");
  recorder_.RegisterCounterMetric("restart_counter", "Counter across restarts");
  recorder_.RegisterSumMetric("restart_sum", "Sum across restarts");
  recorder_.RegisterHistogramMetric("restart_histogram", "Histogram across restarts", {0, 10, 20});
  recorder_.Shutdown();

  for (int restart = 0; restart < 3; ++restart) {
    // Record while stopped. Existing stat objects do not register again.
    recorder_.SetMetricValue("restart_gauge", {{"phase", "restart"}}, 42);
    recorder_.SetMetricValue("restart_counter", {}, 3);
    recorder_.SetMetricValue("restart_sum", {}, 7);
    for (double value : {1, 11, 21}) {
      recorder_.SetMetricValue("restart_histogram", {}, value);
    }
    auto reader = AttachPullReader();
    auto metrics = Collect(*reader);
    ASSERT_EQ(metrics.count("restart_gauge"), 1);
    const auto &gauge = metrics.at("restart_gauge");
    ASSERT_EQ(gauge.point_data_attr_.size(), 1);
    const auto &gauge_point = opentelemetry::nostd::get<LastValuePointData>(
        gauge.point_data_attr_[0].point_data);
    EXPECT_TRUE(gauge_point.is_lastvalue_valid_);
    EXPECT_EQ(opentelemetry::nostd::get<double>(gauge_point.value_), 42);
    EXPECT_EQ(opentelemetry::nostd::get<std::string>(
                  gauge.point_data_attr_[0].attributes.at("phase")), "restart");
    EXPECT_EQ(gauge.instrument_descriptor.description_, "Gauge across restarts");
    const auto &counter = metrics.at("restart_counter");
    EXPECT_EQ(counter.aggregation_temporality, AggregationTemporality::kDelta);
    const auto &counter_point = opentelemetry::nostd::get<SumPointData>(
        counter.point_data_attr_.at(0).point_data);
    EXPECT_TRUE(counter_point.is_monotonic_);
    EXPECT_EQ(opentelemetry::nostd::get<double>(counter_point.value_), 3);
    const auto &sum_point = opentelemetry::nostd::get<SumPointData>(
        metrics.at("restart_sum").point_data_attr_.at(0).point_data);
    EXPECT_FALSE(sum_point.is_monotonic_);
    EXPECT_EQ(opentelemetry::nostd::get<double>(sum_point.value_), 7);
    const auto &histogram = opentelemetry::nostd::get<HistogramPointData>(
        metrics.at("restart_histogram").point_data_attr_.at(0).point_data);
    EXPECT_EQ(histogram.boundaries_, (std::vector<double>{0, 10, 20}));
    EXPECT_EQ(histogram.counts_, (std::vector<uint64_t>{0, 1, 1, 1}));
    EXPECT_EQ(histogram.count_, 3);
    EXPECT_EQ(opentelemetry::nostd::get<double>(histogram.sum_), 33);

    recorder_.SetMetricValue("restart_counter", {}, 2);
    recorder_.SetMetricValue("restart_sum", {}, -2);
    recorder_.SetMetricValue("restart_histogram", {}, 11);
    metrics = Collect(*reader);
    EXPECT_EQ(opentelemetry::nostd::get<double>(opentelemetry::nostd::get<SumPointData>(
                  metrics.at("restart_counter").point_data_attr_.at(0).point_data).value_), 2);
    EXPECT_EQ(opentelemetry::nostd::get<double>(opentelemetry::nostd::get<SumPointData>(
                  metrics.at("restart_sum").point_data_attr_.at(0).point_data).value_), 5);
    const auto &next_histogram = opentelemetry::nostd::get<HistogramPointData>(
        metrics.at("restart_histogram").point_data_attr_.at(0).point_data);
    EXPECT_EQ(next_histogram.count_, 1);
    EXPECT_EQ(opentelemetry::nostd::get<double>(next_histogram.sum_), 11);
    std::weak_ptr<MetricReader> old_reader = reader;
    auto old_provider = GetProvider();
    recorder_.Shutdown();
    EXPECT_TRUE(reader->IsShutdown());
    reader.reset();
    EXPECT_TRUE(old_reader.expired());
    EXPECT_TRUE(old_provider.expired());
  }
}

}  // namespace observability
}  // namespace ray
