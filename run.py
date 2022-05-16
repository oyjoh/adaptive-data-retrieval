import flask
from flask import jsonify, send_file, request

from app.externalresources.datagenerator import Datagenerator


r = [{'source': 'NIVA', 'format': 'NetCDF'}]

app = flask.Flask(__name__)
app.config["DEBUG"] = True


@app.route('/', methods=['GET'])
def home():

    info = {'list cached datasets': '/datasets',
            'download dataset': '/datasets/download/<id>',
            'new dataset, provide url': '/datasets/download/new/<url>'}

    return(jsonify(info))


# List every dataset already cached
@app.route('/datasets', methods=['GET'])
def get_datasets():

    datasets = [
        {'id': 1, 'name': 'GS17_ROV01_LokiCastle_08072017_LLD_depthcorrected.grd', 'size': '26.5 MB'}]
    return jsonify(datasets)


@app.route('/dataset/download/new/<url>', methods=['GET'])
def new_dataset(url):

    dataset_url = url

    # do something smart
    d = ...

    return(d.get_id())


@app.route('/datasets/download/<id>', methods=['GET'])
def download_data(id):

    args = request.args

    parameters = {
        # temporal filters
        'time>=': args.get('time>='),
        'time<=': args.get('time<='),

        # spatial filters
        'lat>': args.get('lat>'),
        'lat<': args.get('lat<'),
        'lng>': args.get('lng>'),
        'lng<': args.get('lng<'),

        # constraints
        'memorylimit': args.get('memorylimit', type=int),

        # possible future constraints, e.g. cesium maps have a limit on max spatial resolution
        'temporal_stepsize': args.get('temporal_stepsize', type=int),
        'spatial_stepsize': args.get('spatial_stepsize', type=int),

        # file format for returned file
        'file_format': args.get('file_format')
    }
    
    print(parameters['file_format'])

    # genrerate dataset based on parameters
    gen = Datagenerator(id, parameters)

    # return netCDF metadata and the netCDF file
    return send_file(gen.generate_dataset(fullres=True))


app.run()
