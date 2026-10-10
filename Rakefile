# frozen_string_literal: true

# ==============================================================================
# Modern Data Architecture POC - Master Rakefile
#
# THERE IS NO LOCAL EXECUTION PROFILE. The platform runs on Kubernetes only
# (kind + Podman, per srs.txt RNF-04): ingestion is a CronJob, the medallion
# layers are SparkApplications, the speed layer is a FlinkDeployment, and
# serving is Trino. There is deliberately no task that executes the pipeline
# locally, because a local run would be a different system pretending to be
# the real one.
#
# What remains local is code verification only (tests, lint, types, contract
# validation) plus cluster operations for the stream tier.
# ==============================================================================

require 'rake'

def preflight_require!(cmd, name, remedy)
  sh cmd
rescue StandardError
  abort "\e[31mPre-flight failed: #{name} check did not run: #{cmd}\e[0m\n\e[33m#{remedy}\e[0m"
end

desc 'Default task: run the code-verification suite (the platform itself runs on Kubernetes)'
task default: %i[governance:validate lint typecheck test]

desc 'Verify system pre-flight dependencies and Python environment'
task :preflight do
  puts "\e[33m[Pre-flight] Environment checks...\e[0m"
  preflight_require!('python --version', 'Python',
                     'Install Python 3.11+ and make sure `python` is on PATH.')
  preflight_require!('rake --version', 'Rake',
                     'Install Rake (gem install rake) and make sure `rake` is on PATH.')
  preflight_require!('python -m pytest --version', 'pytest',
                     'Install the dev toolchain: pip install pytest pytest-cov (pyproject [project.optional-dependencies].dev).')
  puts "\e[32mPython runtime, Rake and pytest verified.\e[0m"
end

desc 'Run all automated tests (unit + integration) with the coverage gate'
task :test do
  puts "\e[33m[Tests] Executing automated suite...\e[0m"
  sh 'python -m pytest tests --cov=src --cov=lineage --cov-branch --cov-fail-under=85 -q'
  puts "\e[32mAll test suites passed.\e[0m"
end

namespace :test do
  desc 'Run unit tests'
  task :unit do
    sh 'python -m pytest tests/unit -v'
  end

  desc 'Run integration tests (RF acceptance scenarios)'
  task :integration do
    sh 'python -m pytest tests/integration -v'
  end

  desc 'Run tests with a coverage report'
  task :coverage do
    sh 'python -m pytest tests --cov=src --cov=lineage --cov-branch --cov-config=pyproject.toml --cov-report=term-missing'
  end
end

desc 'Lint the codebase with ruff'
task :lint do
  puts "\e[33m[Quality] Running ruff...\e[0m"
  sh 'ruff check src tests pipelines governance quality data-generator lineage'
  puts "\e[32mRuff: all checks passed.\e[0m"
end

desc 'Format the codebase with ruff (same directories as lint, writes in place)'
task :format do
  puts "\e[33m[Quality] Running ruff format...\e[0m"
  sh 'ruff format src tests pipelines governance quality data-generator lineage'
  puts "\e[32mRuff format: done.\e[0m"
end

namespace :format do
  desc 'Verify formatting without writing (CI gate: rake format:check)'
  task :check do
    sh 'ruff format --check src tests pipelines governance quality data-generator lineage'
    puts "\e[32mRuff format: all files formatted.\e[0m"
  end
end

desc 'Type-check the codebase with mypy'
task :typecheck do
  puts "\e[33m[Quality] Running mypy...\e[0m"
  sh 'python -m mypy'
  puts "\e[32mMypy: no errors.\e[0m"
end

namespace :governance do
  desc 'Validate Data Products and Contracts across domains (Data Mesh)'
  task :validate do
    puts "\e[33m[Governance] Validating Data Products & Contracts (Data Mesh / ODCS)...\e[0m"
    sh 'python governance/contract_validator.py --repo-root .'
    puts "\e[32mData contracts verified.\e[0m"
  end
end

# ---------------------------------------------------------------------------
# Cluster operations (Kubernetes). These do not run the pipelines; they deploy
# and observe them.
# ---------------------------------------------------------------------------
namespace :cluster do
  desc 'Create the kind cluster, namespaces and prerequisites (bash)'
  task :setup do
    sh 'bash scripts/setup.sh'
  end

  desc 'Deploy a startup profile (args: profile=platform|stream|batch|demo) (bash)'
  task :up, [:profile] do |_t, args|
    profile = args[:profile] || 'stream'
    sh 'bash scripts/profile-up.sh', profile
  end

  desc 'Tear a startup profile down (args: profile=platform|stream|batch|demo) (bash)'
  task :down, [:profile] do |_t, args|
    profile = args[:profile] || 'stream'
    sh 'bash scripts/profile-down.sh', profile
  end
end

namespace :stream do
  desc 'Deploy Kafka + Flink Operator + the speed-layer jobs via Helm (bash)'
  task :deploy do
    puts "\e[33m[S4] Deploying Kafka + Flink Operator + Flink jobs...\e[0m"
    sh 'bash scripts/profile-up.sh stream'
    puts "\e[32mStream tier deployed. Follow with `rake stream:status` and `rake stream:logs`.\e[0m"
  end

  desc 'Show FlinkDeployment status and the speed-layer pods'
  task :status do
    puts "\e[33m[S4] FlinkDeployment status...\e[0m"
    sh 'kubectl get flinkdeployments -A'
    sh 'kubectl get pods -A -l app=flink-orders-stream'
    sh 'kubectl get pods -A -l app=flink-clickstream-stream'
  end

  desc 'Tail the JobManager logs of the orders speed-layer job'
  task :logs do
    puts "\e[33m[S4] Streaming orders job logs...\e[0m"
    sh 'kubectl logs -n sales -l app=flink-orders-stream --tail=200 -f'
  end

  desc 'Publish domain events into the in-cluster Kafka topics'
  task :produce do
    puts "\e[33m[S4] Publishing events to Kafka for 60 s at 20 events/s...\e[0m"
    sh 'python data-generator/event_producer.py --bootstrap-servers localhost:9092 --rate 20 --duration 60'
  end
end

desc 'Clean local caches and temporary files (test fixtures, bytecode)'
task :clean do
  puts 'Cleaning local caches and bytecode...'
  FileList['data/*.db', 'data/*.jsonl', 'lineage_events.db', '.coverage', '.coverage.*',
           '**/*.pyc', '**/.pytest_cache', '**/__pycache__'].each do |f|
    rm_rf f
    puts "Removed #{f}"
  end
  puts "\e[32mClean complete.\e[0m"
end