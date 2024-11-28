{
  "cwlVersion": "v1.0",
  "$namespaces": {
    "s": "https://schema.org/"
  },
  "s:softwareVersion": "1.1.0",
  "schemas": [
    "http://schema.org/version/9.0/schemaorg-current-http.rdf"
  ],
  "$graph": [
    {
      "class": "Workflow",
      "id": "water-bodies",
      "label": "Water bodies detection based on NDWI and otsu threshold",
      "doc": "Water bodies detection based on NDWI and otsu threshold applied to Sentinel-2 COG STAC items",
      "requirements": [
        {
          "class": "ScatterFeatureRequirement"
        },
        {
          "class": "SubworkflowFeatureRequirement"
        }
      ],
      "inputs": {
        "aoi": {
          "label": "area of interest",
          "doc": "area of interest as a bounding box",
          "type": "string"
        },
        "epsg": {
          "label": "EPSG code",
          "doc": "EPSG code",
          "type": "string",
          "default": "EPSG:4326"
        },
        "stac_items": {
          "label": "Sentinel-2 STAC items",
          "doc": "list of Sentinel-2 COG STAC items",
          "type": "string[]"
        },
        "bands": {
          "label": "bands used for the NDWI",
          "doc": "bands used for the NDWI",
          "type": "string[]",
          "default": [
            "green",
            "nir"
          ]
        }
      },
      "outputs": [
        {
          "id": "stac_catalog",
          "outputSource": [
            "node_stac/stac_catalog"
          ],
          "type": "Directory"
        }
      ],
      "steps": {
        "node_water_bodies": {
          "run": "#detect_water_body",
          "in": {
            "item": "stac_items",
            "aoi": "aoi",
            "epsg": "epsg",
            "bands": "bands"
          },
          "out": [
            "detected_water_body"
          ],
          "scatter": "item",
          "scatterMethod": "dotproduct"
        },
        "node_stac": {
          "run": "#stac",
          "in": {
            "item": "stac_items",
            "rasters": {
              "source": "node_water_bodies/detected_water_body"
            }
          },
          "out": [
            "stac_catalog"
          ]
        }
      }
    },
    {
      "class": "Workflow",
      "id": "detect_water_body",
      "label": "Water body detection based on NDWI and otsu threshold",
      "doc": "Water body detection based on NDWI and otsu threshold",
      "requirements": [
        {
          "class": "ScatterFeatureRequirement"
        }
      ],
      "inputs": {
        "aoi": {
          "doc": "area of interest as a bounding box",
          "label": "area of interest",
          "type": "string"
        },
        "epsg": {
          "doc": "EPSG code",
          "label": "EPSG code",
          "type": "string",
          "default": "EPSG:4326"
        },
        "bands": {
          "doc": "bands used for the NDWI",
          "label": "bands used for the NDWI",
          "type": "string[]"
        },
        "item": {
          "doc": "STAC item",
          "label": "STAC item",
          "type": "string"
        }
      },
      "outputs": [
        {
          "id": "detected_water_body",
          "outputSource": [
            "node_otsu/binary_mask_item"
          ],
          "type": "File"
        }
      ],
      "steps": {
        "node_crop": {
          "run": "#crop",
          "in": {
            "item": "item",
            "aoi": "aoi",
            "epsg": "epsg",
            "band": "bands"
          },
          "out": [
            "cropped"
          ],
          "scatter": "band",
          "scatterMethod": "dotproduct"
        },
        "node_normalized_difference": {
          "run": "#norm_diff",
          "in": {
            "rasters": {
              "source": "node_crop/cropped"
            }
          },
          "out": [
            "ndwi"
          ]
        },
        "node_otsu": {
          "run": "#otsu",
          "in": {
            "raster": {
              "source": "node_normalized_difference/ndwi"
            }
          },
          "out": [
            "binary_mask_item"
          ]
        }
      }
    },
    {
      "class": "CommandLineTool",
      "id": "crop",
      "requirements": {
        "InlineJavascriptRequirement": {},
        "EnvVarRequirement": {
          "envDef": {
            "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            "PYTHONPATH": "/app"
          }
        },
        "ResourceRequirement": {
          "coresMax": 1,
          "ramMax": 512
        }
      },
      "hints": {
        "DockerRequirement": {
          "dockerPull": "ghcr.io/eoap/mastering-app-package/crop@sha256:5c623e05fc6cb228848f4ebd89de229be28dc89b36f046ba58fbf3a18af0ae06"
        }
      },
      "baseCommand": [
        "python",
        "-m",
        "app"
      ],
      "arguments": [],
      "inputs": {
        "item": {
          "type": "string",
          "inputBinding": {
            "prefix": "--input-item"
          }
        },
        "aoi": {
          "type": "string",
          "inputBinding": {
            "prefix": "--aoi"
          }
        },
        "epsg": {
          "type": "string",
          "inputBinding": {
            "prefix": "--epsg"
          }
        },
        "band": {
          "type": "string",
          "inputBinding": {
            "prefix": "--band"
          }
        }
      },
      "outputs": {
        "cropped": {
          "outputBinding": {
            "glob": "*.tif"
          },
          "type": "File"
        }
      }
    },
    {
      "class": "CommandLineTool",
      "id": "norm_diff",
      "requirements": {
        "InlineJavascriptRequirement": {},
        "EnvVarRequirement": {
          "envDef": {
            "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            "PYTHONPATH": "/app"
          }
        },
        "ResourceRequirement": {
          "coresMax": 1,
          "ramMax": 512
        }
      },
      "hints": {
        "DockerRequirement": {
          "dockerPull": "ghcr.io/eoap/mastering-app-package/norm_diff@sha256:305f940cb1e86ed6c5491291fc7e7dd55eb42ee7e120c4ca7abf3b3ec99a393d"
        }
      },
      "baseCommand": [
        "python",
        "-m",
        "app"
      ],
      "arguments": [],
      "inputs": {
        "rasters": {
          "type": "File[]",
          "inputBinding": {
            "position": 1
          }
        }
      },
      "outputs": {
        "ndwi": {
          "outputBinding": {
            "glob": "*.tif"
          },
          "type": "File"
        }
      }
    },
    {
      "class": "CommandLineTool",
      "id": "otsu",
      "requirements": {
        "InlineJavascriptRequirement": {},
        "EnvVarRequirement": {
          "envDef": {
            "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            "PYTHONPATH": "/app"
          }
        },
        "ResourceRequirement": {
          "coresMax": 1,
          "ramMax": 512
        }
      },
      "hints": {
        "DockerRequirement": {
          "dockerPull": "ghcr.io/eoap/mastering-app-package/otsu@sha256:5f991d281130971bd3a03aead8ed107cc2a9415bfb5ae84c00607829517bcd84"
        }
      },
      "baseCommand": [
        "python",
        "-m",
        "app"
      ],
      "arguments": [],
      "inputs": {
        "raster": {
          "type": "File",
          "inputBinding": {
            "position": 1
          }
        }
      },
      "outputs": {
        "binary_mask_item": {
          "outputBinding": {
            "glob": "*.tif"
          },
          "type": "File"
        }
      }
    },
    {
      "class": "CommandLineTool",
      "id": "stac",
      "requirements": {
        "InlineJavascriptRequirement": {},
        "EnvVarRequirement": {
          "envDef": {
            "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            "PYTHONPATH": "/app"
          }
        },
        "ResourceRequirement": {
          "coresMax": 1,
          "ramMax": 512
        }
      },
      "hints": {
        "DockerRequirement": {
          "dockerPull": "ghcr.io/eoap/mastering-app-package/stac@sha256:ae88fe9dfcdf4927095b940b4fbf2c03e273b6014d755a5b59c25e238ecfe172"
        }
      },
      "baseCommand": [
        "python",
        "-m",
        "app"
      ],
      "arguments": [],
      "inputs": {
        "item": {
          "type": {
            "type": "array",
            "items": "string",
            "inputBinding": {
              "prefix": "--input-item"
            }
          }
        },
        "rasters": {
          "type": {
            "type": "array",
            "items": "File",
            "inputBinding": {
              "prefix": "--water-body"
            }
          }
        }
      },
      "outputs": {
        "stac_catalog": {
          "outputBinding": {
            "glob": "."
          },
          "type": "Directory"
        }
      }
    }
  ],
  "s:codeRepository": {
    "URL": "https://github.com/eoap/mastering-app-package.git"
  },
  "s:author": [
    {
      "class": "s:Person",
      "s.name": "Jane Doe",
      "s.email": "jane.doe@acme.earth",
      "s.affiliation": "ACME"
    }
  ]
}