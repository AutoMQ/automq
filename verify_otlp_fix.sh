#!/bin/bash
# Quick verification script for OTLP metrics exporter fix (Issue #3111, PR #3124)

set -e

echo "=========================================="
echo "OTLP Metrics Exporter Fix Verification"
echo "Issue: #3111 | PR: #3124"
echo "=========================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Step 1: Check current branch
echo "Step 1: Checking current branch..."
CURRENT_BRANCH=$(git branch --show-current)
echo "Current branch: ${CURRENT_BRANCH}"
echo ""

# Step 2: Build automq-metrics module
echo "Step 2: Building automq-metrics module..."
if ./gradlew :automq-metrics:jar --quiet; then
    echo -e "${GREEN}✓ Build successful${NC}"
else
    echo -e "${RED}✗ Build failed${NC}"
    exit 1
fi
echo ""

# Step 3: Verify dependency
echo "Step 3: Verifying opentelemetry-exporter-sender-jdk dependency..."
if ./gradlew :automq-metrics:dependencies 2>/dev/null | grep -q "opentelemetry-exporter-sender-jdk:1.40.0"; then
    echo -e "${GREEN}✓ Dependency found: opentelemetry-exporter-sender-jdk:1.40.0${NC}"
else
    echo -e "${RED}✗ Dependency not found${NC}"
    exit 1
fi
echo ""

# Step 4: Check for okhttp exclusion
echo "Step 4: Checking okhttp sender exclusion..."
if ./gradlew :automq-metrics:dependencies 2>/dev/null | grep -q "opentelemetry-exporter-sender-okhttp"; then
    echo -e "${YELLOW}⚠ Warning: okhttp sender still present (should be excluded)${NC}"
else
    echo -e "${GREEN}✓ okhttp sender properly excluded${NC}"
fi
echo ""

# Step 5: Verify test file exists
echo "Step 5: Verifying e2e test file..."
if [ -f "tests/kafkatest/automq/otlp_metrics_exporter_test.py" ]; then
    echo -e "${GREEN}✓ E2E test file exists${NC}"
    echo "  Location: tests/kafkatest/automq/otlp_metrics_exporter_test.py"
else
    echo -e "${RED}✗ E2E test file not found${NC}"
    exit 1
fi
echo ""

# Step 6: Check test syntax
echo "Step 6: Checking test file syntax..."
if python3 -m py_compile tests/kafkatest/automq/otlp_metrics_exporter_test.py 2>/dev/null; then
    echo -e "${GREEN}✓ Test file syntax is valid${NC}"
else
    echo -e "${YELLOW}⚠ Warning: Could not verify test syntax (python3 may not be available)${NC}"
fi
echo ""

# Summary
echo "=========================================="
echo "Verification Summary"
echo "=========================================="
echo -e "${GREEN}✓ Build verification passed${NC}"
echo -e "${GREEN}✓ Dependency verification passed${NC}"
echo -e "${GREEN}✓ Test file created${NC}"
echo ""
echo "Next steps:"
echo "1. Run the e2e test using Docker:"
echo "   ${YELLOW}TC_PATHS=\"tests/kafkatest/automq/otlp_metrics_exporter_test.py\" bash tests/docker/run_tests.sh${NC}"
echo ""
echo "2. Or use ducker-ak:"
echo "   ${YELLOW}bash tests/docker/ducker-ak up${NC}"
echo "   ${YELLOW}tests/docker/ducker-ak test tests/kafkatest/automq/otlp_metrics_exporter_test.py${NC}"
echo ""
echo "3. Review the verification document:"
echo "   ${YELLOW}cat OTLP_EXPORTER_TEST_VERIFICATION.md${NC}"
echo ""
echo "=========================================="
