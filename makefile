test:
	python make-test-indices.py
	python dopey.py -c dopey.example.yml
	python confirm-test.py
