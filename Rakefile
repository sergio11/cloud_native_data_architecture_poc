# frozen_string_literal: true

# ==============================================================================
# Modern Data Architecture POC - Master Rakefile
# Replaces bash/powershell scripts with cross-platform Ruby Rake tasks.
# Provides task automation for Medallion pipelines, Quality Gates, Data Contracts,
# Streaming, Unified Serving, Testing, and the full RF-24 Demo.
# ==============================================================================

require 'rake'

# Runs a preflight probe, aborting with an actionable message when it fails.
def preflight_require!(cmd, name, remedy)
  sh cmd
rescue StandardError
  abort "\e[31mPre-flight failed: #{name} check did not run: #{cmd}\e[0m\n\e[33m#{remedy}\e[0m"
end

desc 'Default task: Run full end-to-end automated demo (RF-24)'
task default: :demo

desc 'Run complete RF-24 End-to-End Modern Data Architecture Demonstration'
task :demo do
  puts "\e[34m==================================================================\e[0m"
  puts "\e[34m     MODERN DATA ARCHITECTURE POC - RF-24 END-TO-END DEMO         \e[0m"
  puts "\e[34m==================================================================\e[0m"

  Rake::Task['preflight'].invoke
  Rake::Task['governance:validate'].invoke
  Rake::Task['medallion:bronze'].invoke
  Rake::Task['medallion:silver'].invoke
  Rake::Task['medallion:gold'].invoke
  Rake::Task['stream:orders'].invoke
  Rake::Task['serving:query'].invoke
  Rake::Task['test'].invoke

  puts "\n\e[32m==================================================================\e[0m"
  puts "\e[32m   DEMO COMPLETED SUCCESSFULLY! ALL PARADIGMS VERIFIED.           \e[0m"
  puts "\e[32m==================================================================\e[0m"
end

desc 'Verify system pre-flight dependencies and Python environment'
task :preflight do
  puts "\n\e[33m[Step 1/8] Pre-flight Environment Checks...\e[0m"
  preflight_require!('python --version', 'Python',
                     'Install Python 3.11+ and make sure `python` is on PATH.')
  preflight_require!('rake --version', 'Rake',
                     'Install Rake (gem install rake) and make sure `rake` is on PATH.')
  preflight_require!('python -m pytest --version', 'pytest',
                     'Install the dev toolchain: pip install pytest pytest-cov (see pyproject [project.optional-dependencies].dev).')
  puts "\e[32mPython runtime, Rake and pytest verified.\e[0m"
end

desc 'Run all automated tests (unit + integration)'
task :test do
  puts "\n\e[33m[Step 8/8] Executing Automated Test Suite...\e[0m"
  sh 'python -m pytest tests --cov=src --cov-branch --cov-fail-under=85 -q'
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

  desc 'Run unit tests with test coverage report'
  task :coverage do
    sh 'python -m pytest tests --cov=src --cov-branch --cov-config=pyproject.toml --cov-report=term-missing'
  end
end

desc 'Lint the codebase with ruff'
task :lint do
  puts "\n\e[33m[Quality] Running ruff...\e[0m"
  sh 'ruff check src tests pipelines governance quality data-generator lineage'
  puts "\e[32mRuff: all checks passed.\e[0m"
end

desc 'Format the codebase with ruff (same directories as lint, writes in place)'
task :format do
  puts "\n\e[33m[Quality] Running ruff format...\e[0m"
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

desc 'Type-check the hexagonal core with mypy'
task :typecheck do
  puts "\n\e[33m[Quality] Running mypy...\e[0m"
  sh 'python -m mypy'
  puts "\e[32mMypy: no errors.\e[0m"
end

namespace :governance do
  desc 'Validate Data Products and Contracts across domains (Data Mesh)'
  task :validate do
    puts "\n\e[33m[Step 2/8] Validating Data Products & Contracts (Data Mesh / ODCS)...\e[0m"
    sh 'python governance/contract_validator.py --repo-root .'
    puts "\e[32mData contracts verified.\e[0m"
  end
end

namespace :medallion do
  desc 'Run Bronze layer raw ingestion from OLTP with audit columns'
  task :bronze do
    puts "\n\e[33m[Step 3/8] Running Medallion Architecture: Bronze Ingestion...\e[0m"
    sh 'python pipelines/medallion/bronze_ingestion.py --entity all'
    puts "\e[32mBronze raw ingestion complete.\e[0m"
  end

  desc 'Run Silver layer deduplication, conformance, and quality gates'
  task :silver do
    puts "\n\e[33m[Step 4/8] Running Silver Layer: Cleansing & Quality Gates (RF-06)...\e[0m"
    sh 'python pipelines/medallion/silver_transform.py --entity all'
    puts "\e[32mSilver promotion verified.\e[0m"
  end

  desc 'Run Gold layer dimensional modeling and GDPR PII masking'
  task :gold do
    puts "\n\e[33m[Step 5/8] Running Gold Layer: Business Rollups & GDPR Pseudonymization...\e[0m"
    sh 'python pipelines/medallion/gold_aggregation.py --target all'
    puts "\e[32mGold models published.\e[0m"
  end

  desc 'Run full Medallion pipeline sequentially: Bronze -> Silver -> Gold'
  task all: %i[bronze silver gold]
end

namespace :stream do
  desc 'Process streaming speed layer events and tumbling windows (Kappa S4)'
  task :orders do
    puts "\n\e[33m[Step 6/8] Producing Real-time Events & Computing Tumbling Windows...\e[0m"
    sh 'python pipelines/stream/flink_orders_stream.py --topic sales.orders.events'
    puts "\e[32mStreaming speed buffer updated.\e[0m"
  end

  desc 'Replay historical log from earliest offset (Kappa RF-10)'
  task :replay do
    puts "\n\e[33mTriggering Kappa Historical Stream Replay (RF-10)...\e[0m"
    sh 'python pipelines/stream/flink_orders_stream.py --topic sales.orders.events --start-offset earliest'
    puts "\e[32mStream log replayed.\e[0m"
  end
end

namespace :serving do
  desc 'Execute live unified serving query (RF-11: Batch + Stream)'
  task :query do
    puts "\n\e[33m[Step 7/8] Executing Live Unified Serving Query (RF-11: Batch + Stream)...\e[0m"
    sh 'python pipelines/serving/run_serving_query.py --limit 10'
    puts "\e[32mServing query executed.\e[0m"
  end
end

namespace :quality do
  desc 'Run a quality gate on a table with a suite JSON (args: suite, table, threshold)'
  task :gate, %i[suite table threshold] do |_t, args|
    suite = args[:suite] || 'quality/expectations/silver_orders_suite.json'
    table = args[:table] || 'silver.sales.orders'
    thresh = args[:threshold] || '0.95'
    sh 'python', 'quality/gates/run_quality_gate.py', '--suite', suite, '--table', table, '--threshold', thresh
  end
end

desc 'Clean local database caches and temporary files'
task :clean do
  puts 'Cleaning local databases, cache files, and bytecode...'
  FileList['data/*.db', 'data/*.jsonl', 'lineage_events.db', '.coverage', '.coverage.*',
           '**/*.pyc', '**/.pytest_cache', '**/__pycache__'].each do |f|
    rm_rf f
    puts "Removed #{f}"
  end
  puts "\e[32mClean complete.\e[0m"
end
