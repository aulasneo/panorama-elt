install:
	python -m virtualenv -p python3 venv
	venv/bin/python -m pip install -r requirements.txt
	cp -n panorama_openedx_settings_example.yaml panorama_settings.yaml
