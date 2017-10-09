require 'chewy/type/adapter/orm'

module Chewy
  class Type
    module Adapter
      class ActiveRecord < Orm
        def self.accepts?(target)
          defined?(::ActiveRecord::Base) && (
            target.is_a?(Class) && target < ::ActiveRecord::Base ||
            target.is_a?(::ActiveRecord::Relation))
        end

      private

        def cleanup_default_scope!
          if Chewy.logger && (@default_scope.arel.orders.present? ||
             @default_scope.arel.limit.present? || @default_scope.arel.offset.present?)
            Chewy.logger.warn('Default type scope order, limit and offset are ignored and will be nullified')
          end

          @default_scope = @default_scope.reorder(nil).limit(nil).offset(nil)
        end

        def import_scope(scope, options)
          pluck_in_batches(scope, options.slice(:batch_size)).inject(true) do |result, ids|
            objects = if options[:raw_import]
              raw_default_scope_where_ids_in(ids, options[:raw_import])
            else
              default_scope_where_ids_in(ids)
            end

            result & yield(grouped_objects(objects))
          end
        end

        def primary_key
          @primary_key ||= target.primary_key.to_sym
        end

        def target_id
          target.arel_table[primary_key.to_s]
        end

        def pluck(scope, fields: [], typecast: true)
          if typecast
            scope.except(:includes).distinct.pluck(primary_key, *fields)
          else
            scope = scope.except(:includes).distinct
            scope.select_values = [primary_key, *fields].map do |column|
              target.columns_hash.key?(column) ? target.arel_table[column] : column
            end
            sql = scope.to_sql

            if fields.present?
              target.connection.select_rows(sql)
            else
              target.connection.select_values(sql)
            end
          end
        end

        def pluck_in_batches(scope, fields: [], batch_size: nil, typecast: true)
          return enum_for(:pluck_in_batches, scope, fields: fields, batch_size: batch_size, typecast: typecast) unless block_given?

          id_scope = scope.reorder(target_id.desc).limit(1)
          scope = scope.reorder(target_id.asc).limit(batch_size)
          count = 0
          first_id = 1
          last_id = batch_size
          final_id = pluck(id_scope).first

          while first_id <= final_id
            ids = pluck(scope.where(target_id.between(first_id..last_id)), fields: fields, typecast: typecast)
            yield ids
            first_id = last_id + 1
            last_id += batch_size
            final_id = pluck(id_scope).first
          end

          count
        end

        def scope_where_ids_in(scope, ids)
          scope.where(target_id.in(Array.wrap(ids)))
        end

        def raw_default_scope_where_ids_in(ids, converter)
          sql = default_scope_where_ids_in(ids).to_sql
          object_class.connection.execute(sql).map(&converter)
        end

        def relation_class
          ::ActiveRecord::Relation
        end

        def object_class
          ::ActiveRecord::Base
        end
      end
    end
  end
end
