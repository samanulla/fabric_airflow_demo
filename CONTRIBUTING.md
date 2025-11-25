# Contributing to Fabric Airflow API Client

Thank you for your interest in contributing! This document provides guidelines and instructions for contributing to the project.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Coding Standards](#coding-standards)
- [Testing Guidelines](#testing-guidelines)
- [Pull Request Process](#pull-request-process)
- [Documentation](#documentation)

## Code of Conduct

This project adheres to a code of conduct. By participating, you are expected to:

- Be respectful and inclusive
- Welcome newcomers and help them get started
- Focus on what is best for the community
- Show empathy towards other community members

## Getting Started

### Prerequisites

- Python >= 3.8
- Git
- A Microsoft Fabric workspace with Airflow enabled
- Service Principal with appropriate permissions

### Development Setup

1. **Fork and clone the repository**

```bash
git clone https://github.com/YOUR_USERNAME/fabric-airflow-client.git
cd fabric-airflow-client
```

2. **Create a virtual environment**

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. **Install development dependencies**

```bash
pip install -e ".[test]"
```

4. **Create test configuration**

Copy the example configuration and add your credentials:

```bash
cp tests/config.ini.example tests/config.ini
# Edit tests/config.ini with your test credentials
```

**Important**: Never commit `tests/config.ini` or any file containing credentials.

## Coding Standards

### Python Style Guide

Follow [PEP 8](https://pep8.org/) conventions:

- Use 4 spaces for indentation (no tabs)
- Maximum line length: 100 characters
- Use meaningful variable and function names
- Add docstrings to all public functions and classes

### Type Hints

Always use type hints for better code clarity and IDE support:

```python
def create_file(self, file_path: str, content: Union[str, bytes]) -> ApiResponse:
    """
    Create or update a file.
    
    Args:
        file_path: Path to the file
        content: File content (text or binary)
        
    Returns:
        ApiResponse object with status and body
        
    Raises:
        ValidationError: If file path is invalid
        AuthenticationError: If authentication fails
    """
    pass
```

### Docstrings

Use Google-style docstrings:

```python
def example_function(param1: str, param2: int = 0) -> bool:
    """
    Brief description of the function.
    
    Longer description if needed, explaining the purpose and behavior.
    
    Args:
        param1: Description of param1
        param2: Description of param2 (default: 0)
        
    Returns:
        Description of return value
        
    Raises:
        ValueError: When parameter is invalid
        TypeError: When parameter type is wrong
        
    Example:
        >>> result = example_function("test", 42)
        >>> print(result)
        True
    """
    pass
```

### Import Organization

Organize imports in this order:

1. Standard library imports
2. Third-party imports
3. Local application imports

```python
import os
import typing as t
from pathlib import Path

import msal
import requests

from fabric.airflow.client.base.config import Config
from fabric.airflow.client.base.api_exceptions import APIError
```

### Error Handling

Use specific exception types and provide helpful error messages:

```python
# Good
try:
    response = self._make_request('GET', url)
except requests.RequestException as e:
    raise APIError(f"Failed to fetch file: {e}", status=500)

# Bad
try:
    response = self._make_request('GET', url)
except Exception:
    pass  # Silent failures are bad!
```

### Logging

Use proper logging levels:

```python
import logging

logger = logging.getLogger(__name__)

logger.debug("Detailed diagnostic information")
logger.info("General informational messages")
logger.warning("Warning messages for recoverable issues")
logger.error("Error messages for serious problems")
```

## Testing Guidelines

### Writing Tests

1. **Test file naming**: Use `test_*.py` pattern
2. **Test class naming**: Use `Test*` pattern
3. **Test method naming**: Use `test_*` pattern

```python
import unittest
from fabric.airflow.client.base.config import Config

class TestFilesApiClient(unittest.TestCase):
    """Test cases for Files API client"""
    
    def setUp(self):
        """Set up test fixtures"""
        Config.setup('tests/config.ini')
        self.client = Config.files_client()
    
    def test_create_file(self):
        """Test file creation"""
        response = self.client.create_or_update_file(
            'dags/test.py',
            '# Test DAG'
        )
        self.assertEqual(response.status, 200)
    
    def tearDown(self):
        """Clean up test resources"""
        try:
            self.client.delete_file('dags/test.py')
        except:
            pass
```

### Running Tests

```bash
# Run all tests
python -m unittest discover tests

# Run specific test file
python -m unittest tests.test_files_api_client

# Run specific test
python -m unittest tests.test_files_api_client.TestFilesApiClient.test_create_file

# Run with verbose output
python -m unittest discover tests -v
```

### Test Coverage

Aim for high test coverage:

- All public methods should have tests
- Test both success and failure scenarios
- Test edge cases and error conditions

### Integration Tests

Integration tests require a real Fabric workspace:

- Use a dedicated test workspace
- Clean up resources after tests
- Use unique identifiers (timestamps) to avoid conflicts

```python
import time

class TestIntegration(unittest.TestCase):
    def setUp(self):
        self.timestamp = str(int(time.time()))
        self.test_file = f"dags/test_{self.timestamp}.py"
    
    def tearDown(self):
        # Always clean up
        try:
            self.client.delete_file(self.test_file)
        except:
            pass
```

## Pull Request Process

### Before Submitting

1. **Ensure all tests pass**
```bash
python -m unittest discover tests
```

2. **Check code style**
```bash
# Use a linter if available
pylint src/fabric/airflow/client/
```

3. **Update documentation**
- Add/update docstrings
- Update README.md if adding features
- Update API_REFERENCE.md for API changes

4. **Add tests** for new features

### Submitting a Pull Request

1. **Create a feature branch**
```bash
git checkout -b feature/your-feature-name
```

2. **Make your changes**
- Write clear, concise commit messages
- Keep commits focused on a single change
- Reference issue numbers in commits

```bash
git commit -m "Add support for bulk file upload (#123)"
```

3. **Push to your fork**
```bash
git push origin feature/your-feature-name
```

4. **Create Pull Request**
- Provide a clear description of changes
- Link related issues
- Add screenshots for UI changes
- Request review from maintainers

### Pull Request Template

```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing
- [ ] All tests pass
- [ ] Added new tests
- [ ] Tested manually

## Checklist
- [ ] Code follows style guidelines
- [ ] Self-reviewed the code
- [ ] Commented hard-to-understand areas
- [ ] Updated documentation
- [ ] No new warnings
```

## Documentation

### Code Documentation

- Add docstrings to all public classes, methods, and functions
- Include parameter descriptions, return values, and exceptions
- Provide usage examples in docstrings

### User Documentation

When adding features, update:

- **README.md**: Quick start and overview
- **API_REFERENCE.md**: Detailed API documentation
- **CONFIG_GUIDE.md**: Configuration instructions
- **src/sample/example_usage.py**: Usage examples

### Documentation Style

- Use clear, concise language
- Include code examples
- Provide context and rationale
- Link to related documentation

## Security

### Reporting Security Issues

**Do not** open public issues for security vulnerabilities. Instead:

1. Email the security team directly
2. Provide detailed information about the vulnerability
3. Wait for confirmation before disclosing publicly

### Security Best Practices

- Never commit credentials or secrets
- Use environment variables for sensitive data
- Add sensitive files to `.gitignore`
- Review code for security issues before submitting

```python
# Good - Use environment variables
client_secret = os.getenv('FABRIC_CLIENT_SECRET')

# Bad - Hardcoded credentials
client_secret = "my-secret-key"  # Never do this!
```

## Code Review Process

### For Contributors

- Respond to review comments promptly
- Be open to feedback and suggestions
- Explain reasoning for design decisions
- Update PR based on feedback

### For Reviewers

- Be respectful and constructive
- Focus on code quality and maintainability
- Suggest improvements, not just problems
- Approve when requirements are met

## Release Process

Maintainers follow this process for releases:

1. Update version in `pyproject.toml`
2. Update CHANGELOG.md
3. Create release branch
4. Run full test suite
5. Create GitHub release
6. Publish to PyPI (if applicable)

## Questions?

If you have questions:

- Check existing documentation
- Search closed issues
- Open a new issue with the "question" label
- Contact maintainers

## License

By contributing, you agree that your contributions will be licensed under the project's license.

## Acknowledgments

Thank you for contributing to make this project better! Your time and effort are appreciated.

---

Happy coding! 🚀
