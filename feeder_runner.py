#!/usr/bin/env python3
"""Runner per il Mass Domain Feeder — worker dedicato Railway"""
import asyncio
from mass_feeder import main
print("Starting Industrial Mass Feeder v1.0...")
asyncio.run(main())
