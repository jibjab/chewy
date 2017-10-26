module Chewy
  class Strategy
    # The strategy works the same way as atomic, but performs
    # async index update driven by shoryuken
    #
    #   Chewy.strategy(:shoryuken) do
    #     User.all.map(&:save) # Does nothing here
    #     Post.all.map(&:save) # And here
    #     # It imports all the changed users and posts right here
    #   end
    #
    class Shoryuken < Atomic
      class Worker
        include ::Shoryuken::Worker

        shoryuken_options auto_delete: true,
                          body_parser: :json

        def perform(_sqs_msg, body)
          options = body['options'] || {}
          options[:refresh] = !Chewy.disable_refresh_async if Chewy.disable_refresh_async
          body['type'].constantize.import!(body['ids'], options.deep_symbolize_keys!)
        end
      end

      def leave
        @stash.each do |type, ids|
          next if ids.empty?
          body = {type: type.name, ids: ids}
          Shoryuken::Worker.perform_async(
            body,
            queue: shoryuken_queue,
            message_group_id: type.name,
            message_deduplication_id: Digest::SHA256.hexdigest("#{body}#{Time.zone.now}")
          )
        end
      end

    private

      def shoryuken_queue
        Chewy.settings.fetch(:shoryuken, {})[:queue] || 'chewy'
      end
    end
  end
end
