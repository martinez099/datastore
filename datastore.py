"""
Product

Id : Number
Name : String
Description: String
Vendor : String
Price : Number
Currency : String
MainCategory : Category (1)
Images : Image (0..n)

Category

Id : Number
Name : String
Products : Product (0..n)

Image

Id : Number
Value : Binary
"""


class DataStore(object):

    r = None

    def __init__(self, r):
        self.r = r

    def set_product(self, product_id, values):
        return bool(self.r.hmset('PRODUCT:{}'.format(product_id), values))

    def rem_product(self, product_id):
        return bool(self.r.delete('PRODUCT:{}'.format(product_id)))

    def add_category(self, name):
        category_id = self.r.incr('ID:CATEGORY')
        if self.r.hmset('CATEGORY:{}'.format(category_id), {'Name': name, 'Products': 'PRODUCTS_IN_CATEGORY:{}'.format(category_id)}):
            return category_id

    def rem_category(self, category_id):
        return bool(self.r.delete('CATEGORY:{}'.format(category_id)))

    def add_image(self, value):
        image_id = self.r.incr('ID:IMAGE')
        if self.r.set('IMAGE:{}'.format(image_id), value):
            return image_id

    def rem_image(self, img_id):
        return bool(self.r.delete('IMAGE:{}'.format(img_id)))

    def add_product_to_category(self, product_id, category_id):
        return bool(self.r.lpush('PRODUCTS_IN_CATEGORY:{}'.format(category_id), product_id))

    def rem_product_from_category(self, product_id, category_id):
        return bool(self.r.lrem('PRODUCTS_IN_CATEGORY:{}'.format(category_id), 1, product_id))

    def get_category_id(self, name):
        return self.r.hget('IDX:CATEGORY_NAME', name)

    def add_images_of_product(self, image_ids, product_id):
        return bool(self.r.rpush('IMAGES_OF_PRODUCT:{}'.format(product_id), *image_ids))

    def get_images_of_prduct(self, product_id):
        return self.r.lrange('IMAGES_OF_PRODUCT:{}'.format(product_id), 0, -1)

    def rem_imgages_of_product(self, product_id):
        return bool(self.r.delete('IMAGES_OF_PRODUCT:{}'.format(product_id)))

    def add_product_name_idx(self, product_name, product_id):
        return bool(self.r.hset('IDX:PRODUCT_NAME', product_name, product_id))

    def rem_product_name_idx(self, product_name):
        return bool(self.r.hdel('IDX:PRODUCT_NAME', product_name))

    def incr_category_cnt(self, category_name):
        return bool(self.r.zincrby('CNT:PRODUCT_IN_CATEGORY', category_name))

    def decr_category_cnt(self, category_name):
        return bool(self.r.zincrby('CNT:PRODUCT_IN_CATEGORY', category_name, amount=-1))

    def add_category_name_idx(self, category_name, category_id):
        return bool(self.r.hset('IDX:CATEGORY_NAME', category_name, category_id))

    def rem_category_name_idx(self, category_name):
        return bool(self.r.hdel('IDX:CATEGORY_NAME', category_name))

    def get_product(self, product_id):
        return self.r.hgetall('PRODUCT:{}'.format(product_id))

    def get_category(self, category_id):
        return self.r.hgetall('CATEGORY:{}'.format(category_id))

    def get_image(self, image_id):
        return self.r.get('IMAGE:{}'.format(image_id))

    def get_images_for_product(self, product_id):
        return [self.r.mget('IMAGE:{}'.format(i)) for i in self.r.lrange('IMAGES_OF_PRODUCT:{}'.format(product_id), 0, -1)]
