from deuce.util.map_field import map_field
from unittest import TestCase


class Test_Map_Field(TestCase):

    def setUp(self):
        self.base_dict = {'food': '1'}
        self.changed_dict = {'taco': 0}

    def test_map(self):
        map_field(int, from_dict=self.base_dict, to_dict=self.changed_dict,
                  header_name='food',
                  field_name='taco')

        self.assertEqual(1, self.changed_dict['taco'])

    def test_map_keyerror(self):
        map_field(int, from_dict=self.base_dict, to_dict=self.changed_dict,
                  header_name='food',
                  field_name='burger')

        self.assertEqual(1, self.changed_dict['burger'])

        map_field(int, from_dict=self.base_dict, to_dict=self.changed_dict,
                  header_name='space',
                  field_name='aliens')

        self.assertEqual(0, self.changed_dict['aliens'])

    def test_map_valueerror(self):
        self.base_dict['food'] = 'dont eat me'
        map_field(float, from_dict=self.base_dict, to_dict=self.changed_dict,
                  header_name='food',
                  field_name='aliens')

        self.assertEqual(0.0, self.changed_dict['aliens'])
