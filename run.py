import flask
from flask import jsonify, send_file, request

from app.externalresources.datagenerator import Datagenerator



r = [{'source': 'NIVA', 'format': 'NetCDF'}]

app = flask.Flask(__name__)
app.config["DEBUG"] = True


@app.route('/', methods=['GET'])
def home():

    info = {'list cached datasets': '/datasets',
            'download dataset': '/datasets/download/<id>'}

    return(jsonify(info))


# List every dataset already cached
@app.route('/datasets', methods=['GET'])
def get_datasets():

    datasets = [
        {'id': 1, 'name': 'GS17_ROV01_LokiCastle_08072017_LLD_depthcorrected.grd', 'size': '26.5 MB'}]

    return jsonify(datasets)


@app.route('/datasets/download/<id>', methods=['GET'])
def download_data(id):

    args = request.args

    constraints = {
        'time>=': args.get('time>='),
        'time<=': args.get('time<='),
        'lat>': args.get('lat>'),
        'lat<': args.get('lat<'),
        'lng>': args.get('lng>'),
        'lng<': args.get('lng<'),
        'memorylimit': args.get('memorylimit', type=int)}

    gen = Datagenerator(id, constraints)

    # return netCDF metadata and the netCDF file
    return send_file(gen.generate_dataset(fullres=True))


app.run()
