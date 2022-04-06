visual-template.json: src
	stac planetnicfi create-collection visual visual-template.json \
	   --thumbnail="https://planet-pulse-assets-production.s3.amazonaws.com/uploads/2020/09/nicfi_tropics_basemap_map_d1.jpg" \
	   --extra-field "msft:short_description=Planet's high-resolution, analysis-ready mosaics of the world's tropics" \
	   --extra-field "msft:storage_account=planet" \
	   --extra-field "msft:container=nicfi" \
	   --extra-provider '{"name": "Microsoft", "url": "https://planetarycomputer.microsoft.com/", "roles": ["host"]}'

analytic-template.json: src/stactools/planet_nicfi
	stac planetnicfi create-collection analytic analytic-template.json \
	   --thumbnail="https://planet-pulse-assets-production.s3.amazonaws.com/uploads/2020/09/nicfi_tropics_basemap_map_d1.jpg" \
	   --extra-field "msft:short_description=Planet's high-resolution, analysis-ready mosaics of the world's tropics" \
	   --extra-field "msft:storage_account=planet" \
	   --extra-field "msft:container=nicfi" \
	   --extra-provider '{"name": "Microsoft", "url": "https://planetarycomputer.microsoft.com/", "roles": ["host"]}'