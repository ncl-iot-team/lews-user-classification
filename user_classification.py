from procstream import StreamProcessMicroService
import os
import logging as logger
import csv

config = {"MODULE_NAME": os.environ.get('MODULE_NAME', 'LEWS_USER_CLASSIFICATION'),
          "CONSUMER_GROUP": os.environ.get("CONSUMER_GROUP", "LEWS_USER_CLASSIFICATION_CG"),
          "CLASS_FILENAME": os.environ.get("CLASS_FILENAME", "user_classes.csv")}


class StreamProcessLanguageTranslateService(StreamProcessMicroService):

    def read_class_file(self):
        with open(self.config.get("CLASS_FILENAME"), 'r')as f:
            reader = csv.reader(f, delimiter=",")
            self.class_matrix = []
            for i, class_list in enumerate(reader):
                self.class_matrix.append(class_list)

    def __init__(self, config_new, translator_obj):
        super().__init__(config_new)
        self.translator_obj = translator_obj

    def process_message(self, message):
        payload = message.value
        try:
            screen_name = payload.get('user').get('screen_name')
            classes = []
            for class_list in self.class_matrix:
                if screen_name in class_list[1:]:
                    classes.append(class_list[0])
                    print(f"User:{screen_name} | Class identified: {class_list[0]}")

            if len(classes) > 0:
                payload["lews_meta_user_class"] = classes
            else:
                print("No Class identified for the Twitter handle")
                payload["lews_meta_user_class"] = ['Other']
        except:
            logger.error(f"Cannot classify user:{payload}")
        #print(payload)
        return payload


def main():
    k_service = StreamProcessLanguageTranslateService(config, translator_obj)
    k_service.start_service()


if __name__ == "__main__":
    main()
