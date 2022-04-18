install:
	virtualenv -p python3 venv
	venv/bin/pip install -r requirements.txt
	cp -n panorama_settings_example.yaml panorama_settings.yaml