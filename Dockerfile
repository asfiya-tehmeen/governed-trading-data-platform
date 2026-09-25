FROM python:3.12-slim

WORKDIR /opt/gtdp
COPY pyproject.toml README.md ./
COPY src ./src
COPY contracts ./contracts
RUN pip install --no-cache-dir .

ENV GTDP_CONTRACTS_DIR=/opt/gtdp/contracts
ENTRYPOINT ["python", "-m", "gtdp"]
CMD ["stream"]
