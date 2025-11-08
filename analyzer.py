import pathway as pw
import json

KAFKA_SERVER = 'localhost:9092'

class SimulationResult(pw.Schema):
    angle: float
    velocity: float
    range: float
    flight_time: float

def run():
    print("🧠 Pathway Analyzer starting...")
    results_stream = pw.io.kafka.read(
        KAFKA_SERVER, topic='simulation_results', schema=SimulationResult,
        format="json", autocommit_duration_ms=1000
    )
    live_stats = results_stream.reduce(
        pw.this._id, count=pw.reducers.count(), avg_range=pw.reducers.mean(pw.this.range)
    )
    best_result = results_stream.reduce(pw.this._id, pw.reducers.max_by(pw.this.range))

    pw.io.kafka.write(live_stats, KAFKA_SERVER, topic='dashboard_stats')
    pw.io.kafka.write(best_result, KAFKA_SERVER, topic='dashboard_best_result')
    pw.io.kafka.write(results_stream, KAFKA_SERVER, topic='dashboard_plot_data')

    pw.run()

if __name__ == '__main__':
    run()