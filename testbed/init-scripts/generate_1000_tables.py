#!/usr/bin/env python3
"""
Cienka nakładka na generate_enterprise_schema.py: zakłada 1000 tabel w analytics_db,
embeddingi w pgvector i wpisy w schema_foreign_keys.
"""

import asyncio
from pathlib import Path
import sys

_init_dir = str(Path(__file__).resolve().parent)
if _init_dir not in sys.path:
    sys.path.insert(0, _init_dir)

from generate_enterprise_schema import (
    DatabaseConfig,
    EnterpriseTableDefinition,
    ForeignKeyRelation,
    create_database_connection,
    execute_enterprise_schema_deployment,
    generate_enterprise_schema_definitions,
    get_enterprise_hub_tables,
    initialize_enterprise_metadata_tables,
    load_embedding_model,
    main,
    verify_enterprise_schema,
)

if __name__ == "__main__":
    asyncio.run(main())
