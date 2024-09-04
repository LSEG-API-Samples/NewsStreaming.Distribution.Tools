# =============================================================================
#   Delivery Platform demo app to convert instrument symbology
#-----------------------------------------------------------------------------
#   This source code is provided under the Apache 2.0 license
#   and is provided AS IS with no warranty or guarantee of fit for purpose.
#   Copyright (C) 2024 LSEG. All rights reserved.
# =============================================================================
import requests
import json
import rdpToken

# Application Constants
RDP_version = "/v1"
base_URL = "https://api.refinitiv.com"
category_URL = "/discovery/symbology"
endpoint_URL = "/lookup"



#==============================================
def convertSymbology(requestData):
#==============================================
	RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + endpoint_URL
	dResp = requests.post(RESOURCE_ENDPOINT, headers = {"Authorization": "Bearer " + accessToken}, data=json.dumps(requestData))
	if dResp.status_code != 200:
		print("Unable to get data. Code %s, Message: %s" % (dResp.status_code, dResp.text))
	else:
		# Display data
		jResp = json.loads(dResp.text)
		print(json.dumps(jResp, indent=2))



#==============================================
if __name__ == "__main__":
#==============================================
	# Get latest access token
	print("Getting OAuth access token...")
	accessToken = rdpToken.getToken()
	print("Invoking symbology conversion requests")

	identifier_to_organization_PermID = {
	"from": [
		{
			"identifierTypes": [
				"RIC"
			],
			"values": [
				"IBM.N",
				"LSEG.L"
			]
		}
	],
	"to": [
		{
			"objectTypes": [
				"organization"
			],
			"identifierTypes": [
				"PermID"
			]
		}
	],
	"reference": [
		"name",
		"status",
		"classification"
	],
	"type": "auto"
	}
	print("<identifier_to_organization_PermID>")
	convertSymbology(identifier_to_organization_PermID)


	identifier_to_instrument_PermID = {
	"from": [
		{
			"identifierTypes": [
				"RIC"
			],
			"values": [
				"IBM.N",
				"LSEG.L"
			]
		}
	],
	"to": [
		{
			"objectTypes": [
				"anyinstrument"
			],
			"identifierTypes": [
				"PermID"
			]
		}
	],
	"reference": [
		"name",
		"status",
		"classification"
	],
	"type": "auto"
	}
	print("<identifier_to_instrument_PermID>")
	convertSymbology(identifier_to_instrument_PermID)

	identifier_to_identifier = {
	"from": [
		{
			"identifierTypes": [
				"ISIN"
			],
			"values": [
				"GB00B0SWJX34"
			]
		}
	],
	"to": [
		{
			"identifierTypes": [
				"RIC"
			]
		}
	],
	"reference": [
		"name",
		"status",
		"classification"
	],
	"filter": {
		"status": "active"
	},
	"type": "auto"
	}
	print("<identifier_to_identifier>")
	convertSymbology(identifier_to_identifier)

	identifier_to_an_identifier_at_a_point_of_time = {
	"from": [
		{
			"identifierTypes": [
				"SEDOL"
			],
			"values": [
				"BNTG5Q0"
			]
		}
	],
	"to": [
		{
			"identifierTypes": [
				"RIC"
			]
		}
	],
	"reference": [
		"name",
		"status",
		"classification"
	],
	"type": "auto",
	"effectiveAt": "2021-10-16T04:22:43.000Z"
	}
	print("<identifier_to_an_identifier_at_a_point_of_time>")
	convertSymbology(identifier_to_an_identifier_at_a_point_of_time)
	
	Identifier_to_identifier_history = {
	"from": [
		{
			"identifierTypes": [
				"ISIN"
			],
			"values": [
				"GB00B0SWJX34"
			]
		}
	],
	"to": [
		{
			"identifierTypes": [
				"RIC"
			]
		}
	],
	"showHistory": True,
	"type": "auto"
	}
	print("<Identifier_to_identifier_history>")
	convertSymbology(Identifier_to_identifier_history)

	identifier_to_all_identifiers_for_the_entity = {
	"from": [
		{
			"identifierTypes": [
				"ISIN"
			],
			"values": [
				"US4592001014"
			]
		}
	],
	"to": [
		{
			"identifierTypes": [
				"ANY"
			]
		}
	],
	"reference": [
		"name",
		"status",
		"classification"
	],
	"type": "strict"
	}
	print("<identifier_to_all_identifiers_for_the_entity>")
	convertSymbology(identifier_to_all_identifiers_for_the_entity)

	ISIN_Venue_Identifier_to_RIC = {
	"from": [
		{
			"values": [
				{
					"Isin": "USG5690PAB79",
					"EjvPriceSourceCode": "CPL"
				},
				{
					"MIC": "XLON",
					"Isin": "GB00B0SWJX34"
				}
			]
		}
	],
	"to": [
		{
			"identifierTypes": [
				"RIC"
			]
		}
	],
	"reference": [
		"name"
	],
	"type": "predefined",
	"route": "IsinVenueToQuote"
	}
	print("<ISIN_Venue_Identifier_to_RIC>")
	convertSymbology(ISIN_Venue_Identifier_to_RIC)

	Ticker_Country_to_RIC = {
	"from": [
		{
			"values": [
				{
					"exchangeTicker": "VOD",
					"countryCode": "US"
				}
			]
		}
	],
	"to": [
		{
			"identifierTypes": [
				"RIC"
			]
		}
	],
	"reference": [
		"name",
		"status",
		"classification"
	],
	"type": "predefined",
	"route": "EquityTickerCountryCodeToPrimaryRIC"
	}
	print("<Ticker_Country_to_RIC>")
	convertSymbology(Ticker_Country_to_RIC)
	
	Ticker_MIC_to_Organization = {
	"from": [
		{
			"values": [
				{
					"exchangeTicker": "LSEG",
					"MIC": "XLON"
				}
			]
		}
	],
	"to": [
		{
			"identifierTypes": [
				"PermID"
			],
			"objectTypes": [
				"organization"
			]
		}
	],
	"reference": [
		"name"
	],
	"type": "predefined",
	"route": "ExchangeTickerMicToOrganization"
	}
	print("<Ticker_MIC_to_Organization>")
	convertSymbology(Ticker_MIC_to_Organization)
	
	Corporate_bond_to_ESGStatementParent_mapping = {
	"from": [
		{
			"identifierTypes": [
				"ISIN"
			],
			"values": [
				"USG5690PAB79"
			]
		}
	],
	"reference": [
		"name"
	],
	"type": "predefined",
	"route": "FindESGStatementParent"
	}
	print("<Corporate_bond_to_ESGStatementParent_mapping>")
	convertSymbology(Corporate_bond_to_ESGStatementParent_mapping)
	
	FindPrimaryRIC = {
	"from": [
		{
			"identifierTypes": [
				"Isin"
			],
			"values": [
				"GB0030913577"
			]
		}
	],
	"reference": [
		"name",
		"status",
		"classification"
	],
	"type": "predefined",
	"route": "FindPrimaryRic"
	}
	print("<FindPrimaryRIC>")
	convertSymbology(FindPrimaryRIC)
	
	FindPrimaryRIC_at_point_in_time = {
	"from": [
		{
			"identifierTypes": [
				"PermID"
			],
			"values": [
				"4298007752"
			]
		}
	],
	"reference": [
		"name",
		"status",
		"classification"
	],
	"effectiveAt": "2021-01-28T12:34:33.100Z",
	"type": "predefined",
	"route": "FindPrimaryRic"
	}
	print("<FindPrimaryRIC_at_point_in_time>")
	convertSymbology(FindPrimaryRIC_at_point_in_time)
	
	FindPrimaryRIC_history = {
	"from": [
		{
			"identifierTypes": [
				"Isin"
			],
			"values": [
				"GB00B012T521"
			]
		}
	],
	"reference": [
		"name"
	],
	"showHistory": True,
	"type": "predefined",
	"route": "FindPrimaryRic"
	}
	print("<FindPrimaryRIC_history>")
	convertSymbology(FindPrimaryRIC_history)
	
	Navigation_using_a_single_relationship_path = {
	"from": [
		{
			"identifierTypes": [
				"ISIN"
			],
			"values": [
				"US0231351067"
			]
		}
	],
	"to": [
		{
			"identifierTypes": [
				"RIC"
			]
		}
	],
	"path": [
		{
			"relationshipTypes": [
				"InverseIsValuationQuoteOf"
			],
			"objectTypes": [
				{
					"from": "AnyInstrument",
					"to": "AnyQuote"
				}
			]
		}
	],
	"reference": [
		"name",
		"status",
		"classification"
	],
	"type": "strict"
	}
	print("<Navigation_using_a_single_relationship_path>")
	convertSymbology(Navigation_using_a_single_relationship_path)
	
	Navigation_using_2_relationship_paths = {
	"from": [
		{
			"identifierTypes": [
				"LEI"
			],
			"values": [
				"549300561UZND4C7B569"
			]
		}
	],
	"to": [
		{
			"identifierTypes": [
				"RIC"
			]
		}
	],
	"path": [
		{
			"relationshipTypes": [
				"InverseIsPrimarySecurityOf"
			],
			"objectTypes": [
				{
					"from": "Organization",
					"to": "AnyInstrument"
				}
			]
		},
		{
			"relationshipTypes": [
				"InverseIsValuationQuoteOf"
			],
			"objectTypes": [
				{
					"from": "AnyInstrument",
					"to": "AnyQuote"
				}
			]
		}
	],
	"reference": [
		"name",
		"status",
		"classification"
	],
	"type": "strict"
	}
	print("<Navigation_using_2_relationship_paths>")
	convertSymbology(Navigation_using_2_relationship_paths)
	
	Organization_to_Primary_Fundamental_Series = {
	"from": [
		{
			"objectTypes": [
				"organization"
			],
			"values": [
				"4298007752"
			]
		}
	],
	"to": [
		{
			"objectTypes": [
				"fundamentalseries"
			],
			"identifierTypes": [
				"PermID"
			]
		}
	],
	"path": [
		{
			"relationshipTypes": [
				"PrimaryFundamentalSeries"
			],
			"objectTypes": [
				{
					"from": "organization",
					"to": "fundamentalseries"
				}
			]
		}
	],
	"type": "strict"
	}
	print("<Organization_to_Primary_Fundamental_Series>")
	convertSymbology(Organization_to_Primary_Fundamental_Series)
