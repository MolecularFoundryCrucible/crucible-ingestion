#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec  9 16:04:29 2025

@author: roncofaber
"""

import sys

sys.path.append("/home/roncofaber/Software/crucible-rabbit-mq/ingestion-consumer/ingestors/")
sys.path.append("/home/roncofaber/Software/crucible-rabbit-mq/")

from lmp_ingestor import LmpIngestor

#%%

inp_file = "/home/roncofaber/WORK/AI4micro/01_lmp2curx/CrucibleUpload/02_H2O_bulk_25x25x25/input.lmp"

dataset = LmpIngestor(inp_file)