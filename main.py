import json
import os

from redis import StrictRedis
from flask import request

from datastore import DataStore

from flask import Flask
app = Flask(__name__)

port = os.getenv("PORT")
vcap_services = os.getenv("VCAP_SERVICES")
if vcap_services:
    vcap_services = json.loads(vcap_services)
    port = int(port)

    #r_host = vcap_services["redislabs"][0]["credentials"]["host"]
    r_host = vcap_services["redislabs"][0]["credentials"]["ip_list"][0]
    r_port = vcap_services["redislabs"][0]["credentials"]["port"]
    r_pass = vcap_services["redislabs"][0]["credentials"]["password"]

    r = StrictRedis(decode_responses=True, host=r_host, port=r_port, password=r_pass)

else:

    r = StrictRedis(decode_responses=True)

ds = DataStore(r)


@app.route('/product/<product_id>', methods=['GET'])
def GET_product(product_id):
    values = ds.get_product(product_id)
    values['MainCategory'] = ds.get_category(values['MainCategory'])
    values['Images'] = ds.get_images_for_product(product_id)

    return json.dumps(values)


@app.route('/product', methods=['POST'])
def POST_product():
    values = json.loads(request.data)

    # generate product id
    product_id = r.incr('ID:PRODUCT')

    # add category
    category_id = ds.get_category_id(values['MainCategory'])
    if not category_id:
        category_id = ds.add_category(values['MainCategory'])
    ds.add_category_name_idx(values['MainCategory'], category_id)
    ds.incr_category_cnt(values['MainCategory'])
    values['MainCategory'] = category_id

    # add images
    img_ids = [ds.add_image(img) for img in values['Images']]
    values['Images'] = 'IMAGES_OF_PRODUCT:{}'.format(product_id)

    # add product
    ds.add_images_of_product(img_ids, product_id)
    ds.add_product_to_category(product_id, category_id)
    ds.add_product_name_idx(values['Name'], product_id)
    ds.set_product(product_id, values)

    return json.dumps(product_id)


@app.route('/product/<product_id>', methods=['PUT'])
def PUT_product(product_id):
    new_values = json.loads(request.data)

    # product to be updated
    old_values = ds.get_product(product_id)

    # remove category
    ds.decr_category_cnt(ds.get_category(old_values['MainCategory']))
    ds.rem_category_name_idx(ds.get_category(old_values['MainCategory']))

    # add category
    category_id = ds.get_category_id(new_values['MainCategory'])
    if not category_id:
        category_id = ds.add_category(new_values['MainCategory'])
    ds.add_category_name_idx(new_values['MainCategory'], category_id)
    ds.incr_category_cnt(new_values['MainCategory'])
    new_values['MainCategory'] = category_id

    # remove images
    for img in ds.get_images_of_prduct(product_id):
        ds.rem_image(img)

    # add images
    img_ids = [ds.add_image(img) for img in new_values['Images']]
    new_values['Images'] = 'IMAGES_OF_PRODUCT:{}'.format(product_id)

    # remove product
    ds.rem_imgages_of_product(product_id)
    ds.rem_product_name_idx(old_values['Name'])
    ds.rem_product_from_category(product_id, old_values['MainCategory'])

    # add product
    ds.add_images_of_product(img_ids, product_id)
    ds.add_product_to_category(product_id, category_id)
    ds.add_product_name_idx(new_values['Name'], product_id)

    updated = bool(ds.set_product(product_id, new_values))

    return json.dumps(updated)


@app.route('/product/<product_id>', methods=['DELETE'])
def DELETE_prodcut(product_id):

    # product to be deleted
    values = ds.get_product(product_id)

    # category
    ds.decr_category_cnt(ds.get_category(values['MainCategory']))
    ds.rem_category_name_idx(ds.get_category(values['MainCategory']))

    # images
    img_ids = ds.get_images_of_prduct(product_id)
    for img_id in img_ids:
        ds.rem_image(img_id)

    # product
    ds.rem_product_name_idx(values['Name'])
    ds.rem_product_from_category(product_id, values['MainCategory'])
    ds.rem_imgages_of_product(product_id)
    deleted = bool(ds.rem_product(product_id))

    return json.dumps(deleted)


@app.route('/category/<category_id>', methods=['GET'])
def GET_category(category_id=None):
    return json.dumps(ds.get_category(category_id))


@app.route('/image/<image_id>', methods=['GET'])
def GET_image(image_id=None):
    return ds.get_image(image_id)


@app.route('/search/<term>', methods=['GET'])
def GET_search(term):
    return json.dumps([r.hgetall('PRODUCT:{}'.format(idx[1])) for idx in r.hscan_iter('IDX:PRODUCT_NAME', '*{}*'.format(term))])


@app.route('/list/<category_id>', methods=['GET'])
def GET_list_by_category(category_id):
    return json.dumps([r.hgetall('PRODUCT:{}'.format(idx)) for idx in r.lrange('PRODUCTS_IN_CATEGORY:{}'.format(category_id), 0, -1)])


@app.route('/list', methods=['GET'])
def GET_list():
    return json.dumps(r.zrangebyscore('CNT:PRODUCT_IN_CATEGORY', '-Inf', '+Inf'))


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port)
