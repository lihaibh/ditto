import * as joi from "joi";
import { MongoConnectorSchemaError } from "../errors";

export abstract class Validatable {
    validate() {
        // validating the options
        const { error } = this.schema().validate(this.options(), { abortEarly: true });

        if (error) {
            throw new MongoConnectorSchemaError(error);
        }
    }

    abstract options(): any;
    abstract schema(): joi.Schema;
}