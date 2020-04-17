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
    """
    Data Store class.
    """

    redis = None

    def __init__(self, _redis):
        """
        :param _redis: A Redis instance.
        """
        self.redis = _redis

    def new_product_id(self):
        """
        Add and get a new product ID.

        :return: The new product ID.
        """
        product_id = self.redis.incr('seq:product')
        self.redis.sadd('products', product_id)

        return product_id

    def set_product(self, product_id, values):
        """
        Set a product.

        :param product_id: The ID of the product.
        :param values: A propertiy dict of the product.
        :return: Success.
        """
        return bool(self.redis.hmset('product:{}'.format(product_id), values))

    def get_product(self, product_id):
        """
        Get a product.

        :param product_id: The ID of the product.
        :return: The property dict of the product.
        """
        product = self.redis.hgetall('product:{}'.format(product_id))
        product['Images'] = self.get_product_images(product_id)

        return product

    def rem_product(self, product_id):
        """
        Delete a product.

        :param product_id: The ID of the product.
        :return: Success.
        """
        return bool(self.redis.delete('product:{}'.format(product_id))) \
            and bool(self.redis.srem('products', product_id))

    def new_category_id(self):
        """
        Add and get a new category ID.

        :return: The new category ID.
        """
        category_id = self.redis.incr('seq:category')
        self.redis.sadd('categories', category_id)

        return category_id

    def set_category(self, category_id, name):
        """
        Set a category.

        :param category_id: The category ID.
        :param name: The category name.
        :return: Success.
        """
        return bool(self.redis.hmset('category:{}'.format(category_id),
                    {'Name': name, 'Products': 'category:{}:products'.format(category_id)}))

    def get_category(self, category_id):
        """
        Get a category.

        :param category_id: The category ID.
        :return: The property dict of the category.
        """
        return self.redis.hgetall('category:{}'.format(category_id))

    def rem_category(self, category_id):
        """
        Delete a category.

        :param category_id: The ID of the category.
        :return: Success.
        """
        return bool(self.redis.delete('category:{}'.format(category_id))) and \
            bool(self.redis.srem('categories', category_id))

    def get_category_id(self, name):
        """
        Get the ID of a category.

        :param name: The name of the category.
        :return: The ID of the category.
        """
        return self.redis.hget('idx:category_name', name)

    def new_image_id(self):
        """
        Add and get a new image ID.

        :return: The new image ID.
        """
        image_id = self.redis.incr('seq:image')
        self.redis.sadd('images', image_id)

        return image_id

    def set_image(self, image_id, value):
        """
        Set an image.

        :param image_id: The ID of the image.
        :param value: The property dict of the image.
        :return: Success.
        """
        return bool(self.redis.set('image:{}'.format(image_id), value))

    def get_image(self, image_id):
        """
        Get an image.

        :param image_id: The ID of the image.
        :return: The property dict of the image.
        """
        return self.redis.get('image:{}'.format(image_id))

    def rem_image(self, img_id):
        """
        Delete an image.

        :param img_id: The ID of the image.
        :return: Success.
        """
        return bool(self.redis.delete('image:{}'.format(img_id))) and \
            bool(self.redis.srem('images', img_id))

    def get_iamge_id(self, image):
        """
        Get the ID of an image.

        :param image: The bytestring of the image.
        :return: The ID of the image or None.
        """
        image_ids = self.redis.smembers('images')
        image_id = list(filter(lambda x: x == image, [self.get_image(img_id) for img_id in image_ids]))

        return image_id[0] if image_id else None

    def add_product_to_category(self, product_id, category_id):
        """
        Add a product to a category.

        :param product_id: The ID of the product.
        :param category_id: The ID of the category.
        :return: Success.
        """
        return bool(self.redis.sadd('category:{}:products'.format(category_id), product_id))

    def rem_product_from_category(self, product_id, category_id):
        """
        Remove a product form a category.

        :param product_id: The ID of the product.
        :param category_id: The ID of the category.
        :return: Success.
        """
        return bool(self.redis.srem('category:{}:products'.format(category_id), 1, product_id))

    def add_product_image_ids(self, image_ids, product_id):
        """
        Add images to a product.

        :param image_ids: The IDs of the images.
        :param product_id: The ID of the product.
        :return: Success.
        """
        return bool(self.redis.sadd('product:{}:images'.format(product_id), *image_ids))

    def get_product_image_ids(self, product_id):
        """
        Get all image IDs of a product.

        :param product_id: The ID of the product.
        :return: The list with IDs of the images.
        """
        return self.redis.smembers('product:{}:images'.format(product_id))

    def rem_product_images(self, product_id):
        """
        Remove all images of a product.

        :param product_id: The ID of the product.
        :return: Success.
        """
        return bool(self.redis.delete('product:{}:images'.format(product_id)))

    def rem_images_from_product(self, product_id, image_ids):
        """
        Remove an image of a product.

        :param product_id: The ID of the product.
        :param image_ids: The IDs of the images.
        :return: Success.
        """
        return bool(self.redis.srem('product:{}:images'.format(product_id), *image_ids))

    def get_product_images(self, product_id):
        """
        Get all images of a product.

        :param product_id: The ID of the product.
        :return: The list of images.
        """
        return [self.redis.get('image:{}'.format(i)) for i in
                self.redis.smembers('product:{}:images'.format(product_id))]

    def incr_category_rnk(self, category_name):
        """
        Increment a category->name rank.

        :param category_name: The name of the category.
        :return: Success.
        """
        return bool(self.redis.zincrby('rnk:products_in_category', 1, category_name))

    def decr_category_rnk(self, category_name):
        """
        Decrement a category-name rank.

        :param category_name: The name of the category.
        :return: Success.
        """
        return bool(self.redis.zincrby('rnk:products_in_category', -1, category_name))

    def list_categories(self):
        """
        Get a list of all categories.

        :return: The list of all categories ordered by popularity.
        """
        return self.redis.zrevrange('rnk:products_in_category', 0, -1)

    def list_products(self, category_id):
        """
        Get all products of a category.

        :param category_id: The ID of the category.
        :return: A list with all products of a category.
        """
        return [self.redis.hgetall('product:{}'.format(idx)) for idx in
                self.redis.smembers('category:{}:products'.format(category_id))]

    def set_category_name_idx(self, category_name, category_id):
        """
        Create/update a category->name index.

        :param category_name: The name of the category.
        :param category_id: The ID of the category.
        :return: Success.
        """
        return bool(self.redis.hset('idx:category_name', category_name, category_id))

    def rem_category_name_idx(self, category_name):
        """
        Delete a category->name index.

        :param category_name: The name of the category.
        :return: Success.
        """
        return bool(self.redis.hdel('idx:category_name', category_name))

    def set_product_name_idx(self, product_name, product_id):
        """
        Create/update a product->name index.

        :param product_name: The name of the product.
        :param product_id: The ID of the product.
        :return: Success.
        """
        return bool(self.redis.hset('idx:product_name', product_name, product_id))

    def rem_product_name_idx(self, product_name):
        """
        Delete a product->name index.

        :param product_name: The name of the product.
        :return: Success.
        """
        return bool(self.redis.hdel('idx:product_name', product_name))

    def search_products(self, term):
        """
        Search products by name.

        :param term: The search term.
        :return: The list with products having :param term: in the name.
        """
        return [self.redis.hgetall('product:{}'.format(idx[1])) for idx in
                self.redis.hscan_iter('idx:product_name', '*{}*'.format(term))]
