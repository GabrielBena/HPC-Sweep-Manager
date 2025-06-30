# HSM v2 Test Suite User Guide

## Quick Start

### Running Tests

```bash
# Activate the environment
conda activate nca

# Run all fast tests (recommended)
make test-fast

# Run specific test categories
make test-unit           # Unit tests only
make test-integration    # Integration tests only
make test-cli            # CLI tests only

# Run with coverage
make test-coverage

# Run specific test file
pytest tests/unit/test_parameter_generator.py -v

# Run specific test
pytest tests/integration/test_simple_mlp_documentation.py::TestSimpleMlpDocumentation::test_readme_quick_start -v
```

### Understanding Test Categories

#### 🎯 Unit Tests (tests/unit/)
**Purpose**: Test individual components in isolation
**Status**: ✅ 100% passing
**Speed**: Fast (<1 second)

```bash
# Run unit tests
pytest tests/unit/ -v
```

**What they test**:
- Parameter generation logic
- Configuration loading and validation
- Data structure correctness

#### 🔄 Integration Tests (tests/integration/)
**Purpose**: Test component interactions and workflows
**Status**: ✅ Mostly passing
**Speed**: Medium (5-30 seconds)

```bash
# Run integration tests
pytest tests/integration/ -v
```

**What they test**:
- Simple MLP example execution
- Configuration file loading
- CLI command integration
- Documentation accuracy

#### 🖥️ CLI Tests (tests/cli/)
**Purpose**: Test command-line interface
**Status**: 🚧 In progress
**Speed**: Fast (<5 seconds)

```bash
# Run CLI tests
pytest tests/cli/ -v
```

**What they test**:
- Command parsing and execution
- Error handling
- Help system
- Configuration management commands

## Test Organization

### By Test Type
```
tests/
├── unit/           # Fast, isolated component tests
├── integration/    # Multi-component workflow tests  
└── cli/           # Command-line interface tests
```

### By Functionality
```
tests/
├── *parameter_generator*  # Parameter combination generation
├── *sweep_config*        # Configuration management
├── *simple_mlp*          # Example validation
└── *config_cli*         # Configuration commands
```

## Test Markers

Tests are organized with pytest markers:

```bash
# Run only unit tests
pytest -m unit

# Run only integration tests  
pytest -m integration

# Skip slow tests
pytest -m "not slow"

# Run only CLI tests
pytest -m cli
```

### Available Markers
- `unit`: Fast, isolated tests
- `integration`: Multi-component tests
- `cli`: Command-line interface tests
- `slow`: Tests that take >30 seconds
- `remote`: Tests requiring SSH/remote access
- `hpc`: Tests requiring HPC environment

## Understanding Test Results

### ✅ Passing Tests
Tests pass when:
- All assertions succeed
- No exceptions are raised
- Expected outputs match actual outputs

### ❌ Failing Tests
Common failure types:

1. **AssertionError**: Expected vs actual mismatch
   ```
   assert result.exit_code == 0
   AssertionError: assert 1 == 0
   ```

2. **TypeError**: Object type mismatch
   ```
   TypeError: 'NoneType' object is not subscriptable
   ```

3. **ImportError**: Missing dependencies
   ```
   ImportError: No module named 'some_package'
   ```

### 🚧 Skipped Tests
Tests are skipped when:
- Required files are missing
- Dependencies are unavailable
- Environment conditions aren't met

## Example-Based Testing

### Simple MLP Validation
Our test suite heavily uses the `simple_mlp` example:

```bash
# Test that simple_mlp works as documented
pytest tests/integration/test_simple_mlp_documentation.py -v

# Test simple_mlp integration with HSM
pytest tests/integration/test_simple_mlp_integration.py -v
```

**Why this approach?**
- Validates real user workflows
- Ensures documentation accuracy
- Provides concrete examples of usage

### Documentation Tests
These tests ensure examples work exactly as documented:

- ✅ README quick start commands
- ✅ Parameter override examples  
- ✅ Hydra configuration features
- ✅ Output directory structure
- ✅ Requirements.txt accuracy

## Debugging Test Failures

### Step 1: Understand the Error
```bash
# Run with detailed output
pytest tests/failing_test.py -v -s

# Show local variables on failure
pytest tests/failing_test.py --tb=long

# Stop at first failure
pytest tests/failing_test.py -x
```

### Step 2: Isolate the Issue
```bash
# Test just one function
pytest tests/test_file.py::TestClass::test_method -v

# Test with print statements (use -s flag)
pytest tests/test_file.py -s
```

### Step 3: Check Dependencies
```bash
# Verify environment
python -c "import hsm; print('HSM imported successfully')"

# Check simple_mlp example
cd examples/simple_mlp && python main.py --help
```

## Writing New Tests

### Unit Test Template
```python
import pytest
from hsm.module import ComponentToTest

class TestComponentToTest:
    """Test the ComponentToTest class."""
    
    def test_basic_functionality(self):
        """Test basic component functionality."""
        component = ComponentToTest()
        result = component.do_something()
        assert result is not None
        
    def test_error_handling(self):
        """Test component error handling."""
        component = ComponentToTest()
        with pytest.raises(ValueError):
            component.do_invalid_thing()
```

### Integration Test Template
```python
import pytest
from pathlib import Path

@pytest.mark.integration
class TestWorkflow:
    """Test complete workflow."""
    
    def test_end_to_end_workflow(self, simple_mlp_dir):
        """Test complete user workflow."""
        # Use simple_mlp_dir fixture
        assert (simple_mlp_dir / "main.py").exists()
        
        # Test workflow steps
        # ... test code here ...
```

### CLI Test Template
```python
import pytest
from click.testing import CliRunner
from hsm.cli.main import cli

@pytest.mark.cli
class TestCLICommand:
    """Test CLI command functionality."""
    
    def test_command_help(self, cli_runner):
        """Test command help output."""
        result = cli_runner.invoke(cli, ["command", "--help"])
        assert result.exit_code == 0
        assert "help text" in result.output
```

## Continuous Integration

### Local Testing Workflow
```bash
# Before committing changes
make test-fast              # Run fast tests
make lint                   # Check code style
make format-check           # Check formatting

# Before major releases
make test                   # Run all tests
make test-coverage          # Check coverage
```

### Test Requirements
- Python 3.8+
- All dependencies in requirements.txt
- Access to examples/simple_mlp
- Conda environment (for some tests)

## Troubleshooting

### Common Issues

1. **"pytest not found"**
   ```bash
   conda activate nca
   pip install pytest
   ```

2. **"simple_mlp not found"**
   ```bash
   # Ensure you're in the project root
   ls examples/simple_mlp/
   ```

3. **"HSM module not found"**
   ```bash
   pip install -e .
   ```

4. **Tests hang or take too long**
   ```bash
   # Run without slow tests
   pytest -m "not slow"
   ```

### Getting Help

1. Check test output carefully
2. Run tests with `-v` for verbose output
3. Use `--tb=long` for detailed error traces
4. Test individual components first
5. Verify simple_mlp example works directly

## Best Practices

### For Test Writers
- Use descriptive test names
- Test both success and failure cases
- Use fixtures for common setup
- Keep tests independent
- Add appropriate markers

### For Test Runners
- Run tests frequently during development
- Use appropriate test categories
- Don't ignore warnings
- Check coverage regularly
- Validate examples work

### For Debugging
- Start with unit tests
- Use simple_mlp for integration testing
- Check console output carefully
- Verify environment setup
- Test incrementally

---

**Remember**: The test suite is designed to give you confidence that HSM v2 works correctly. When tests pass, you can trust the functionality. When they fail, they help you identify issues quickly. 