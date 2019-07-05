
# Run pep8 + pyflakes checks
flake8:
	@echo "#### PEP8/pyflakes issues"
	@flake8 .
	@echo "No issues found."


beautiful:
	isort -y
	black .
	flake8 .

